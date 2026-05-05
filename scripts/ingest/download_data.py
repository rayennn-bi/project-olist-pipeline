import os
import shutil
import json
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi

def download_and_prepare():
    api = KaggleApi()
    api.authenticate()

    dataset = "olistbr/brazilian-ecommerce"
    base_path = r"C:\Mini Proyek\PROJECT-OLIST-PIPELINE\data"
    raw_path = os.path.join(base_path, "raw")
    checkpoint_file = os.path.join(base_path, "download_checkpoint.json")

    # Cek checkpoint - apakah sudah download sebelumnya?
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            download_time = checkpoint_data.get('download_time')
            dataset_name = checkpoint_data.get('dataset')
            
            print(f"[CHECKPOINT] Dataset sudah didownload sebelumnya pada: {download_time}")
            print(f"[CHECKPOINT] Dataset: {dataset_name}")
            print("[SKIP] Melewati proses download...")
            
            # Langsung ke proses organize file
            organize_files(base_path, raw_path)
            return
            
        except Exception as e:
            print(f"[WARNING] Error membaca checkpoint: {e}")
            print("[CONTINUE] Melanjutkan proses download...")

    os.makedirs(raw_path, exist_ok=True)

    # Download
    print(f"[DOWNLOAD] Memulai download dataset: {dataset}")
    api.dataset_download_files(dataset, path=base_path, unzip=True)

    print('[DONE] Download finished:', base_path, flush=True)

    # Buat checkpoint setelah download berhasil
    checkpoint_data = {
        'dataset': dataset,
        'download_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'completed',
        'base_path': base_path
    }
    
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
    
    print(f"[CHECKPOINT] Checkpoint dibuat: {checkpoint_file}")

    # Organize files
    organize_files(base_path, raw_path)

def organize_files(base_path, raw_path):
    """Fungsi terpisah untuk organize files"""
    
    # Mapping dataset
    mapping = {
        "orders": "olist_orders_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "items": "olist_order_items_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "order_review": "olist_order_reviews_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "product_category": "product_category_name_translation.csv"
    }

    # Move file ke folder masing-masing
    for folder, filename in mapping.items():
        folder_path = os.path.join(raw_path, folder)
        os.makedirs(folder_path, exist_ok=True)

        src = os.path.join(base_path, filename)
        dst = os.path.join(folder_path, filename)

        if os.path.exists(src):
            shutil.move(src, dst)
            print(f"[MOVE] {filename} -> {folder}/")
        else:
            print(f"[WARNING] File tidak ditemukan: {filename}")

    print('[DONE] File dataset sudah dirapikan:', base_path, flush=True)

if __name__ == "__main__":
    download_and_prepare()