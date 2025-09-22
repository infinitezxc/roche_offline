import json
import os
import glob
from pymilvus import Collection, connections, utility

from utils.config import config


def load_mapping_files():
    """Load mapping files for kb_id, file_id, and img_file_list"""
    # Load id_mapping.json
    with open("data/id_mapping.json", "r") as f:
        id_mapping = json.load(f)

    # Load img_file_list from 0922.json
    with open("data/0922.json", "r") as f:
        img_data = json.load(f)

    return id_mapping, img_data


def process_data_files(data_dir: str, id_mapping: dict, img_data: dict):
    """Process all JSON files in data_dir and convert to Milvus records"""
    records = []

    # Get all JSON files in the data directory
    json_files = glob.glob(os.path.join(data_dir, "*.json"))

    for json_file in json_files:
        print(f"Processing {json_file}...")

        with open(json_file, "r") as f:
            data = json.load(f)

        for key, value in data.items():
            # Extract base key (remove "ocr_results:ocr_" prefix if present)
            base_key = key.replace("ocr_results:ocr_", "") if key.startswith("ocr_results:ocr_") else key

            # Get kb_id and file_id from mapping
            if base_key in id_mapping:
                kb_id = id_mapping[base_key]["kb_id"]
                file_id = id_mapping[base_key]["file_id"]
            else:
                print(f"Warning: {base_key} not found in id_mapping, skipping...")
                continue

            # Get img_file_list from img_data (using original key with prefix)
            img_file_list = img_data.get(key, {}).get("img_file_list", [])

            # Process each page
            processed_content = value.get("processed_content", [])
            embeddings = value.get("embeddings", [])
            sparse_embeddings = value.get("sparse_embeddings", [])

            for i in range(len(processed_content)):
                # Create record for each page
                record = {
                    "kb_id": kb_id,
                    "file_id": file_id,
                    "file_name": value.get("file_name", ""),
                    "image_file": img_file_list[i],
                    "content": processed_content[i],
                    "metadata": value.get("metadata", {}).get("entity", ""),
                    "update_time": value.get("metadata", {}).get("time", ""),
                    "enabled": True,
                    "embedding": embeddings[i],
                    "sparse_vector": sparse_embeddings[i]
                }
                records.append(record)

    return records


def load_data(collection_name: str, data_dir: str = "data/index"):
    """Load processed data from data_dir into Milvus collection"""
    collection = Collection(name=collection_name)
    collection.load()

    # Load mapping files
    id_mapping, img_data = load_mapping_files()

    # Process all data files
    records = process_data_files(data_dir, id_mapping, img_data)

    # Insert data in batches of 2560
    batch_size = 1280
    total_items = len(records)

    print(f"Total records to insert: {total_items}")

    for i in range(0, total_items, batch_size):
        batch = records[i : i + batch_size]
        collection.insert(batch)
        print(
            f"Inserted batch {i // batch_size + 1}/{(total_items + batch_size - 1) // batch_size} "
            f"({len(batch)} items)"
        )

    collection.flush()
    print(f"Data loaded to {collection_name} (total: {total_items} items)")


if __name__ == "__main__":
    connections.connect("default", uri=config.milvus_uri, db_name="default")

    # List all collections first
    print("Available collections:")
    collections = utility.list_collections()
    for i, collection_name in enumerate(collections, 1):
        print(f"{i}. {collection_name}")

    load_data("")