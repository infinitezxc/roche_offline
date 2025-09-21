#!/usr/bin/env python3
"""
SFTP directory downloader with detail.json processing, file renaming, and PDF conversion

Features:
- Downloads files from SFTP server recursively
- Processes detail.json files and converts content to PDF
- Converts Word/Excel/TXT files to PDF using LibreOffice
- Merges multiple images from image folders into a single PDF
- Renames attachments with Chinese naming convention
- Handles file/image subfolder organization

Dependencies:
- paramiko: SFTP client
- Pillow (PIL): Image processing for PDF merging
- LibreOffice: Document conversion (must be installed on system)

Usage:
python get_sftp_policy.py
"""

import os
import json
import paramiko
import subprocess
import time
import gc
import signal
import psutil
from PIL import Image


def convert_in_background(input_file, output_file):
    """Start background conversion process using external PDF converter"""
    subprocess.Popen([
        'python', 'pdf_converter.py', input_file, output_file
    ])


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

        # Use a more isolated LibreOffice command with additional flags for better timeout handling
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

        # Use shorter timeout and better process handling
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,  # Reduced from 60 to 30 seconds
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None  # Create new process group
        )

        if result.returncode == 0:
            # Get the expected output filename
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

        # Kill the timed-out process and any children
        try:
            if hasattr(e, 'process') and e.process:
                # Kill the process group to ensure all child processes are terminated
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

        # Clean up any remaining LibreOffice processes
        kill_hanging_libreoffice()

        # Wait a bit before allowing next conversion to avoid interference
        time.sleep(2)
        return None

    except Exception as e:
        print(f"  [ERROR] LibreOffice conversion error for {os.path.basename(input_file)}: {e}")
        # Clean up any hanging processes on unexpected errors too
        kill_hanging_libreoffice()
        return None


def merge_images_to_pdf(image_files, output_pdf_path):
    """Merge multiple images into a single PDF with each image as one page"""
    if not image_files:
        return False

    try:
        images = []
        for image_file in image_files:
            img = Image.open(image_file)
            # Convert to RGB if necessary (for PNG with transparency)
            if img.mode != 'RGB':
                img = img.convert('RGB')
            images.append(img)

        # Save as PDF with multiple pages
        if images:
            images[0].save(output_pdf_path, save_all=True, append_images=images[1:])
            print(f"  [PDF] Merged {len(images)} images into {os.path.basename(output_pdf_path)}")
            return True
    except Exception as e:
        print(f"  [ERROR] Failed to merge images to PDF: {e}")
        return False


def download_and_convert_file_with_soffice(sftp, remote_file_path, local_folder_path, output_filename):
    """Download a file from SFTP and convert to PDF using LibreOffice"""
    file_name = os.path.basename(remote_file_path)
    temp_file_path = os.path.join(local_folder_path, f"temp_{file_name}")
    final_pdf_path = os.path.join(local_folder_path, output_filename)

    # Check if PDF already exists
    if os.path.exists(final_pdf_path):
        print(f"  [SKIP] PDF already exists: {output_filename}")
        return True

    try:
        # Download to temporary location with timeout handling
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
            # Rename to the desired output filename
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


def download_and_convert_file(sftp, remote_file_path, local_folder_path, output_filename):
    """Download a file from SFTP and start background conversion to PDF"""
    file_name = os.path.basename(remote_file_path)
    temp_file_path = os.path.join(local_folder_path, f"temp_{file_name}")
    final_pdf_path = os.path.join(local_folder_path, output_filename)

    # Check if PDF already exists
    if os.path.exists(final_pdf_path):
        print(f"  [SKIP] PDF already exists: {output_filename}")
        return True

    try:
        # Download to temporary location with timeout handling
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

        # Start background conversion
        convert_in_background(temp_file_path, final_pdf_path)
        print(f"  [BACKGROUND] Started conversion: {file_name} -> {output_filename}")
        return True

    except Exception as e:
        print(f"  [ERROR] Failed to download {file_name}: {e}")
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass
        return False


def download_and_merge_images(sftp, image_folder_path, local_folder_path, output_filename):
    """Download all images from a folder and merge them into a single PDF"""
    final_pdf_path = os.path.join(local_folder_path, output_filename)

    # Check if PDF already exists
    if os.path.exists(final_pdf_path):
        print(f"  [SKIP] PDF already exists: {output_filename}")
        return True

    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
    temp_image_files = []

    try:
        # Get all image files from the folder with timeout handling
        try:
            image_files = sftp.listdir(image_folder_path)
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"  [TIMEOUT] List timeout for {image_folder_path}, retrying...")
                time.sleep(1)
                image_files = sftp.listdir(image_folder_path)
            else:
                raise

        for file_name in image_files:
            file_ext = os.path.splitext(file_name)[1].lower()

            # Only process image files
            if file_ext in image_extensions:
                file_remote_path = f"{image_folder_path}/{file_name}"
                temp_file_path = os.path.join(local_folder_path, f"temp_{file_name}")

                try:
                    # Download with timeout retry
                    try:
                        sftp.get(file_remote_path, temp_file_path)
                        print(f"  [DOWNLOAD] Downloaded image {file_name}")
                        temp_image_files.append(temp_file_path)
                    except Exception as e:
                        if "timeout" in str(e).lower():
                            print(f"  [TIMEOUT] Image download timeout for {file_name}, retrying...")
                            time.sleep(1)
                            sftp.get(file_remote_path, temp_file_path)
                            temp_image_files.append(temp_file_path)
                        else:
                            raise
                except Exception as e:
                    print(f"  [ERROR] Failed to download {file_name}: {e}")

        # Merge all images into one PDF
        if temp_image_files:
            # Sort files to ensure consistent order
            temp_image_files.sort()

            success = merge_images_to_pdf(temp_image_files, final_pdf_path)
            if success:
                print(f"  [MERGE] Created merged PDF from {len(temp_image_files)} images: {output_filename}")
            return success

        return False

    except Exception as e:
        print(f"  [ERROR] Error processing image folder: {e}")
        return False

    finally:
        # Clean up temporary image files and force garbage collection
        for temp_file in temp_image_files:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
        # Force memory cleanup after processing images
        gc.collect()


def process_detail_json(sftp, detail_json_path, folder_name, local_folder_path):
    """Process detail.json file: extract meta and content, start background conversion to PDF"""
    pdf_file_path = os.path.join(local_folder_path, f"{folder_name}.pdf")

    # Check if folder is already processed by checking if main PDF exists
    if os.path.exists(pdf_file_path):
        print(f"  [SKIP] Folder already processed: {folder_name}")
        return True

    temp_json_path = None
    temp_txt_path = None

    try:
        # Download detail.json to temporary location
        temp_json_path = os.path.join(local_folder_path, "temp_detail.json")
        sftp.get(detail_json_path, temp_json_path)

        # Read and process JSON
        with open(temp_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Extract meta and content
        meta = data.get('meta', '')
        content = data.get('content', '')

        # Ensure meta and content are strings
        meta_str = str(meta) if meta else ''
        content_str = str(content) if content else ''

        # Concatenate meta and content
        combined_text = f"{meta_str}\n\n{content_str}" if meta_str and content_str else meta_str or content_str

        # Save as temporary txt file
        temp_txt_path = os.path.join(local_folder_path, f"temp_{folder_name}.txt")
        with open(temp_txt_path, 'w', encoding='utf-8') as f:
            f.write(combined_text)

        # Start background conversion
        convert_in_background(temp_txt_path, pdf_file_path)
        print(f"  [BACKGROUND] Started conversion: detail.json -> {folder_name}.pdf")
        return True

    except Exception as e:
        print(f"  [ERROR] Failed to process detail.json: {e}")
        # Clean up temp files on failure
        for temp_file in [temp_json_path, temp_txt_path]:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
        return False

    finally:
        # Clean up JSON temp file immediately
        if temp_json_path and os.path.exists(temp_json_path):
            try:
                os.remove(temp_json_path)
            except:
                pass


def check_attachment_pdfs_exist(local_folder_path, folder_name):
    """Check if the first attachment PDF already exists for this folder"""
    # Check for pattern: {folder_name}_附件1.pdf
    attachment1_path = os.path.join(local_folder_path, f"{folder_name}_附件1.pdf")
    return os.path.exists(attachment1_path)


def download_and_rename_attachments(sftp, remote_folder_path, local_folder_path, folder_name):
    """Download files from 'file' and 'image' subfolders, convert to PDF"""
    attachment_counter = 1

    # Process 'file' subfolder - handle images and docx files properly
    file_subfolder_path = f"{remote_folder_path}/file"
    try:
        files = sftp.listdir(file_subfolder_path)
        if files:
            # Define file type extensions
            image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
            office_extensions = {'.docx', '.doc', '.pptx', '.ppt'}

            # Separate files by type
            image_files = [f for f in files if os.path.splitext(f)[1].lower() in image_extensions]
            office_files = [f for f in files if os.path.splitext(f)[1].lower() in office_extensions]
            other_files = [f for f in files if os.path.splitext(f)[1].lower() not in image_extensions and os.path.splitext(f)[1].lower() not in office_extensions]

            # Process image files - merge into one PDF if multiple
            if image_files:
                if len(image_files) > 1:
                    # Multiple images - merge them
                    output_filename = f"{folder_name}_附件{attachment_counter}.pdf"
                    success = download_and_merge_images(sftp, file_subfolder_path, local_folder_path, output_filename)
                    if success:
                        attachment_counter += 1
                else:
                    # Single image - convert individually
                    for file_name in image_files:
                        file_remote_path = f"{file_subfolder_path}/{file_name}"
                        output_filename = f"{folder_name}_附件{attachment_counter}.pdf"

                        # For single images, we can use the merge function with one file
                        temp_files = []
                        try:
                            temp_file_path = os.path.join(local_folder_path, f"temp_{file_name}")
                            sftp.get(file_remote_path, temp_file_path)
                            temp_files.append(temp_file_path)

                            success = merge_images_to_pdf(temp_files, os.path.join(local_folder_path, output_filename))
                            if success:
                                attachment_counter += 1
                        except Exception as e:
                            print(f"  [ERROR] Failed to process single image {file_name}: {e}")
                        finally:
                            # Clean up temp files
                            for temp_file in temp_files:
                                if os.path.exists(temp_file):
                                    try:
                                        os.remove(temp_file)
                                    except:
                                        pass

            # Process office files individually using LibreOffice
            for file_name in office_files:
                file_remote_path = f"{file_subfolder_path}/{file_name}"
                output_filename = f"{folder_name}_附件{attachment_counter}.pdf"

                success = download_and_convert_file_with_soffice(sftp, file_remote_path, local_folder_path, output_filename)
                if success:
                    attachment_counter += 1

            # Process other files using background converter
            for file_name in other_files:
                file_remote_path = f"{file_subfolder_path}/{file_name}"
                output_filename = f"{folder_name}_附件{attachment_counter}.pdf"

                success = download_and_convert_file(sftp, file_remote_path, local_folder_path, output_filename)
                if success:
                    attachment_counter += 1

    except FileNotFoundError:
        # File subfolder doesn't exist, skip
        pass
    except Exception as e:
        print(f"  [ERROR] Error accessing file folder: {e}")

    # Process 'image' subfolder - merge all images into one PDF
    image_subfolder_path = f"{remote_folder_path}/image"
    try:
        # Check if image subfolder exists and has files
        image_files = sftp.listdir(image_subfolder_path)
        if image_files:
            output_filename = f"{folder_name}_附件{attachment_counter}.pdf"
            success = download_and_merge_images(sftp, image_subfolder_path, local_folder_path, output_filename)
            if success:
                attachment_counter += 1

    except FileNotFoundError:
        # Image subfolder doesn't exist, skip
        pass
    except Exception as e:
        print(f"  [ERROR] Error accessing image folder: {e}")


def download_sftp_directory_recursive(sftp, remote_path, local_path, folder_name=None, level1_folder=None, is_level1=False, monitor=None):
    """Recursively download directory contents with special processing for detail.json"""
    try:
        # Create local directory only for level 1 folders
        if is_level1:
            os.makedirs(local_path, exist_ok=True)

        # List remote directory contents with timeout handling
        try:
            items = sftp.listdir(remote_path)
        except Exception as e:
            print(f"  [ERROR] Failed to list directory {remote_path}: {e}")
            return

        print(f"Processing directory: {remote_path} ({len(items)} items)")

        # Check if detail.json exists in current directory
        has_detail_json = 'detail.json' in items

        if has_detail_json and folder_name and level1_folder:
            print(f"  Found detail.json in {folder_name}")

            # Use level 1 folder as the target for saving files
            # Extract the base local directory and append level1_folder
            base_local_dir = local_path.split('/ori/')[0] if '/ori/' in local_path else os.path.dirname(local_path)
            level1_local_path = os.path.join(base_local_dir, 'ori', level1_folder)
            os.makedirs(level1_local_path, exist_ok=True)

            # Quick check: if main PDF exists, skip detail.json processing but still check attachments
            main_pdf_path = os.path.join(level1_local_path, f"{folder_name}.pdf")
            main_pdf_exists = os.path.exists(main_pdf_path)

            # Check if any attachment PDFs already exist
            attachments_exist = check_attachment_pdfs_exist(level1_local_path, folder_name)

            if attachments_exist:
                print(f"  [SKIP] Folder fully processed (main PDF and attachments exist): {folder_name}")
                return
            elif main_pdf_exists:
                print(f"  [SKIP] Main PDF exists, checking for missing attachments: {folder_name}")

            # Check connection health before processing
            if monitor:
                _, sftp = monitor.check_and_reconnect_if_needed()

            # Process detail.json file and save to level 1 folder (only if main PDF doesn't exist)
            if not main_pdf_exists:
                detail_json_path = f"{remote_path}/detail.json"
                try:
                    process_detail_json(sftp, detail_json_path, folder_name, level1_local_path)
                except Exception as e:
                    print(f"  [ERROR] Failed to process detail.json: {e}")

            # Download and rename attachments (only if no attachments exist)
            if not attachments_exist:
                try:
                    download_and_rename_attachments(sftp, remote_path, level1_local_path, folder_name)
                except Exception as e:
                    print(f"  [ERROR] Failed to download attachments: {e}")

            return  # Stop processing this directory further

        # Continue with normal recursive processing for other items
        for item in items:
            # Skip folders containing "已废止" in their name
            if "已废止" in item or "失效" in item:
                print(f"  [SKIP] Skipping folder with '已废止' or '失效': {item}")
                continue

            # Skip specific level 1 folder
            if is_level1 and item == "浙江医疗保障局":
                print(f"  [SKIP] Skipping specified folder: {item}")
                continue

            # Check connection health periodically
            if monitor:
                _, sftp = monitor.check_and_reconnect_if_needed()

            remote_item_path = f"{remote_path}/{item}"
            local_item_path = os.path.join(local_path, item)

            try:
                # Check if item is a directory with timeout handling
                stat = sftp.stat(remote_item_path)
                if stat.st_mode & 0o040000:  # Directory
                    print(f"  [DIR]  {item}")

                    # Determine if this is level 1 and set the level1_folder reference
                    if is_level1:
                        # This is a level 1 folder, save its name for future reference
                        download_sftp_directory_recursive(sftp, remote_item_path, local_item_path, item, item, False, monitor)
                    else:
                        # Pass down the level1_folder reference
                        download_sftp_directory_recursive(sftp, remote_item_path, local_item_path, item, level1_folder, False, monitor)
                else:
                    # Handle regular files at level 1 (when no detail.json)
                    if not has_detail_json and is_level1:
                        # Queue file for conversion using extracted function
                        file_stem = os.path.splitext(item)[0]
                        output_filename = f"{file_stem}.pdf"
                        output_pdf_path = os.path.join(local_path, output_filename)

                        # Check if PDF already exists
                        if os.path.exists(output_pdf_path):
                            print(f"  [SKIP] PDF already exists: {output_filename}")
                            continue

                        print(f"  [FILE] Processing {item}")
                        try:
                            success = download_and_convert_file(sftp, remote_item_path, local_path, output_filename)
                            if not success:
                                print(f"  [ERROR] Failed to download {item}")
                        except Exception as e:
                            print(f"  [ERROR] Failed to download {item}: {e}")

            except Exception as e:
                print(f"  [ERROR] Failed to process {item}: {e}")

    except Exception as e:
        print(f"Error processing directory {remote_path}: {e}")


def download_sftp_directory(hostname, username, private_key_path, remote_dir, local_dir):
    """Download directory from SFTP server with connection management and timeout handling"""

    def create_connection():
        """Create a new SFTP connection with proper timeout settings"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Load private key
            private_key = paramiko.RSAKey.from_private_key_file(private_key_path)

            # Connect with reasonable timeouts for long operations
            ssh.connect(
                hostname=hostname,
                username=username,
                pkey=private_key,
                timeout=60,         # 60s connection timeout
                banner_timeout=60,  # 60s banner timeout
                auth_timeout=60     # 60s auth timeout
            )

            # Open SFTP session with longer timeout for large files
            sftp = ssh.open_sftp()
            sftp.get_channel().settimeout(300)  # 5 minutes for individual file operations

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
            sftp.listdir('.')  # Simple test command
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

        # Enhanced download with periodic connection health checks
        try:
            # Add connection health monitoring
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

                    # Check every 100 files or every 30 minutes
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

            # Start download with the original connection
            download_sftp_directory_recursive(
                sftp, remote_dir, local_dir, None, None, True, monitor
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
    remote_dir = "PolicyInitial_202508"
    local_dir = "./ori"
    download_sftp_directory(hostname, username, private_key_path, remote_dir, local_dir)