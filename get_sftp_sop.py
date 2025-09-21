#!/usr/bin/env python3
"""
SFTP directory downloader with office file to PDF conversion

Features:
- Downloads files from SFTP server recursively
- Maintains level 1 folder structure
- Converts Word/Excel/PowerPoint files to PDF using LibreOffice
- Merges multiple images from folders into a single PDF
- Handles file organization without JSON processing

Dependencies:
- paramiko: SFTP client
- Pillow (PIL): Image processing for PDF merging
- LibreOffice: Document conversion (must be installed on system)

Usage:
python get_sftp_sop.py
"""

import os
import paramiko
import subprocess
import time
import gc


def convert_with_libreoffice(input_file, output_dir):
    """Convert document to PDF using LibreOffice headless mode"""
    import signal
    import psutil

    def kill_hanging_libreoffice():
        """Kill any hanging LibreOffice processes"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if proc.info['name'] and 'soffice' in proc.info['name'].lower():
                    cmdline = proc.info['cmdline'] or []
                    if any('--headless' in arg for arg in cmdline):
                        print(f"  [CLEANUP] Killing hanging LibreOffice process: {proc.info['pid']}")
                        proc.kill()
                        proc.wait(timeout=5)
        except Exception as e:
            print(f"  [WARNING] Could not clean up LibreOffice processes: {e}")

    try:
        # Kill any existing hanging LibreOffice processes first
        kill_hanging_libreoffice()

        cmd = [
            'soffice',
            '--headless',
            '--invisible',
            '--nodefault',
            '--nolockcheck',
            '--nologo',
            '--norestore',
            '--convert-to', 'pdf',
            '--outdir', output_dir,
            input_file
        ]

        print(f"  [CONVERT] Starting LibreOffice conversion: {os.path.basename(input_file)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None
        )

        if result.returncode == 0:
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            output_file = os.path.join(output_dir, f"{base_name}.pdf")
            if os.path.exists(output_file):
                print(f"  [SUCCESS] LibreOffice conversion completed: {os.path.basename(output_file)}")
                return output_file
            else:
                print(f"  [ERROR] LibreOffice conversion succeeded but output file not found: {base_name}.pdf")
                return None
        else:
            print(f"  [ERROR] LibreOffice conversion failed with code {result.returncode}: {result.stderr.strip()}")
            return None

    except subprocess.TimeoutExpired as e:
        print(f"  [TIMEOUT] LibreOffice conversion timeout for {os.path.basename(input_file)} (30s)")

        try:
            if hasattr(e, 'process') and e.process:
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(e.process.pid), signal.SIGTERM)
                    time.sleep(2)
                    os.killpg(os.getpgid(e.process.pid), signal.SIGKILL)
                else:
                    e.process.terminate()
                    time.sleep(2)
                    e.process.kill()
        except Exception as kill_error:
            print(f"  [WARNING] Could not kill timed-out process: {kill_error}")

        kill_hanging_libreoffice()
        time.sleep(2)
        return None

    except Exception as e:
        print(f"  [ERROR] LibreOffice conversion error for {os.path.basename(input_file)}: {e}")
        kill_hanging_libreoffice()
        return None




def download_and_convert_file(sftp, remote_file_path, local_folder_path, output_filename):
    """Download a file from SFTP and convert to PDF using LibreOffice"""
    file_name = os.path.basename(remote_file_path)
    temp_file_path = os.path.join(local_folder_path, f"temp_{file_name}")
    final_pdf_path = os.path.join(local_folder_path, output_filename)

    # Check if PDF already exists
    if os.path.exists(final_pdf_path):
        print(f"  [SKIP] PDF already exists: {output_filename}")
        return True

    try:
        # Download to temporary location
        try:
            sftp.get(remote_file_path, temp_file_path)
            print(f"  [DOWNLOAD] Downloaded {file_name}")
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"  [TIMEOUT] Download timeout for {file_name}, retrying...")
                time.sleep(1)
                sftp.get(remote_file_path, temp_file_path)
            else:
                raise

        # Convert using LibreOffice
        converted_pdf = convert_with_libreoffice(temp_file_path, local_folder_path)
        if converted_pdf and os.path.exists(converted_pdf):
            if converted_pdf != final_pdf_path:
                os.rename(converted_pdf, final_pdf_path)
            print(f"  [CONVERT] Converted {file_name} -> {output_filename}")
            return True
        else:
            print(f"  [ERROR] Failed to convert {file_name} with LibreOffice")
            return False

    except Exception as e:
        print(f"  [ERROR] Failed to download/convert {file_name}: {e}")
        return False

    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass




def download_sftp_directory_recursive(sftp, remote_path, local_path, level1_folder=None, is_level1=False, monitor=None, path_prefix=""):
    """Recursively download directory contents with office file conversion"""
    try:
        # Create local directory only for level 1
        if is_level1:
            os.makedirs(local_path, exist_ok=True)

        # List remote directory contents
        try:
            items = sftp.listdir(remote_path)
        except Exception as e:
            print(f"  [ERROR] Failed to list directory {remote_path}: {e}")
            return

        print(f"Processing directory: {remote_path} ({len(items)} items)")

        # Define file type extensions
        office_extensions = {'.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls'}
        pdf_extensions = {'.pdf'}

        for item in items:
            # Check connection health periodically
            if monitor:
                _, sftp = monitor.check_and_reconnect_if_needed()

            remote_item_path = f"{remote_path}/{item}"

            try:
                # Check if item is a directory
                stat = sftp.stat(remote_item_path)
                if stat.st_mode & 0o040000:  # Directory
                    print(f"  [DIR]  {item}")

                    if is_level1:
                        # This is a level 1 folder - create it and process contents
                        level1_local_path = os.path.join(local_path, item)
                        os.makedirs(level1_local_path, exist_ok=True)
                        download_sftp_directory_recursive(sftp, remote_item_path, level1_local_path, item, False, monitor, "")
                    else:
                        # Deeper level - append folder name to path prefix
                        new_prefix = f"{path_prefix}{item}_" if path_prefix else f"{item}_"
                        download_sftp_directory_recursive(sftp, remote_item_path, local_path, level1_folder, False, monitor, new_prefix)
                else:
                    # Handle regular files
                    file_ext = os.path.splitext(item)[1].lower()
                    file_stem = os.path.splitext(item)[0]

                    # Apply path prefix to filename
                    final_filename = f"{path_prefix}{file_stem}" if path_prefix else file_stem

                    if file_ext in office_extensions:
                        # Convert office files to PDF
                        output_filename = f"{final_filename}.pdf"
                        output_pdf_path = os.path.join(local_path, output_filename)

                        # Ensure local directory exists
                        os.makedirs(local_path, exist_ok=True)

                        if os.path.exists(output_pdf_path):
                            print(f"  [SKIP] PDF already exists: {output_filename}")
                            continue

                        print(f"  [OFFICE] Processing {item} -> {output_filename}")
                        try:
                            success = download_and_convert_file(sftp, remote_item_path, local_path, output_filename)
                            if not success:
                                print(f"  [ERROR] Failed to convert {item}")
                        except Exception as e:
                            print(f"  [ERROR] Failed to process {item}: {e}")

                    elif file_ext in pdf_extensions:
                        # Download PDF files directly with flattened name
                        output_filename = f"{final_filename}.pdf"
                        local_item_path = os.path.join(local_path, output_filename)

                        # Ensure local directory exists
                        os.makedirs(local_path, exist_ok=True)

                        if os.path.exists(local_item_path):
                            print(f"  [SKIP] PDF already exists: {output_filename}")
                            continue

                        print(f"  [PDF] Downloading {item} -> {output_filename}")
                        try:
                            sftp.get(remote_item_path, local_item_path)
                            print(f"  [DOWNLOAD] Downloaded {output_filename}")
                        except Exception as e:
                            print(f"  [ERROR] Failed to download {item}: {e}")

                    else:
                        # Ignore other file types
                        print(f"  [IGNORE] Skipping {item} (unsupported file type)")

            except Exception as e:
                print(f"  [ERROR] Failed to process {item}: {e}")

    except Exception as e:
        print(f"Error processing directory {remote_path}: {e}")


def download_sftp_directory(hostname, username, private_key_path, remote_dir, local_dir):
    """Download directory from SFTP server with connection management"""

    def create_connection():
        """Create a new SFTP connection with proper timeout settings"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Load private key
            private_key = paramiko.RSAKey.from_private_key_file(private_key_path)

            # Connect with reasonable timeouts
            ssh.connect(
                hostname=hostname,
                username=username,
                pkey=private_key,
                timeout=60,
                banner_timeout=60,
                auth_timeout=60
            )

            # Open SFTP session
            sftp = ssh.open_sftp()
            sftp.get_channel().settimeout(300)  # 5 minutes for file operations

            return ssh, sftp

        except Exception as e:
            print(f"Connection failed: {e}")
            if 'ssh' in locals():
                try:
                    ssh.close()
                except:
                    pass
            raise

    def safe_close_connection(ssh, sftp):
        """Safely close SFTP and SSH connections"""
        try:
            if sftp:
                sftp.close()
        except:
            pass

        try:
            if ssh:
                ssh.close()
        except:
            pass

    def test_connection(sftp):
        """Test if connection is still alive"""
        try:
            sftp.listdir('.')
            return True
        except:
            return False

    ssh = None
    sftp = None

    try:
        print(f"Connecting to {hostname} as {username}")
        ssh, sftp = create_connection()
        print("Connected successfully")

        print(f"Starting recursive download from {remote_dir} to {local_dir}")

        # Connection health monitoring
        class ConnectionMonitor:
            def __init__(self, ssh, sftp, create_conn_func):
                self.ssh = ssh
                self.sftp = sftp
                self.create_connection = create_conn_func
                self.last_check = time.time()
                self.files_since_check = 0

            def check_and_reconnect_if_needed(self):
                """Check connection health every 100 files or 30 minutes"""
                current_time = time.time()
                self.files_since_check += 1

                if (self.files_since_check >= 100 or
                    current_time - self.last_check > 1800):  # 30 minutes

                    print(f"  [HEALTH] Checking connection health after {self.files_since_check} files...")

                    if not test_connection(self.sftp):
                        print("  [RECONNECT] Connection lost, reconnecting...")
                        safe_close_connection(self.ssh, self.sftp)
                        time.sleep(2)
                        gc.collect()

                        self.ssh, self.sftp = self.create_connection()
                        print("  [RECONNECT] Successfully reconnected")
                    else:
                        print("  [HEALTH] Connection is healthy")

                    self.last_check = current_time
                    self.files_since_check = 0

                    return self.ssh, self.sftp

                return self.ssh, self.sftp

        # Create connection monitor
        monitor = ConnectionMonitor(ssh, sftp, create_connection)

        # Start download
        download_sftp_directory_recursive(
            sftp, remote_dir, local_dir, None, True, monitor, ""
        )
        print("Download completed successfully")

    except Exception as e:
        print(f"Error during download: {e}")
        raise

    finally:
        safe_close_connection(ssh, sftp)


if __name__ == "__main__":
    hostname = "s-726a57061b9f4aa09.server.transfer.cn-north-1.amazonaws.com.cn"
    username = "zhous63-Prod"
    private_key_path = "/home/zxc/.ssh/key.pem"
    remote_dir = "SOPInitial_202509"
    local_dir = "./data/sop"
    download_sftp_directory(hostname, username, private_key_path, remote_dir, local_dir)