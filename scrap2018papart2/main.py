
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import pytz
import time
import os
import random

# Zona waktu WIB
tz_wib = pytz.timezone('Asia/Jakarta')
def current_time():
    return datetime.now(tz_wib).strftime("%Y-%m-%d %H:%M:%S %Z")

# Rotasi User-Agent
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
]

# Inisialisasi session
session = requests.Session()
session.headers.update({'User-Agent': random.choice(user_agents)})

# Buat folder output jika belum ada
os.makedirs('csv', exist_ok=True)

# Helper untuk ambil data dari tabel detail
def get_text_after_label(soup, label_text):
    label = soup.find('td', string=label_text)
    return label.find_next('td').text.strip() if label else 'N/A'

# Ambil link PDF
def get_pdf_link(soup):
    pdf_link = soup.find('a', href=True, string=lambda x: x and x.endswith('.pdf'))
    return pdf_link['href'] if pdf_link else 'N/A'

# Parameter tetap
year = '2018'
category = 'regis'
direktori = 'perdata-agama'

# Daftar pengadilan
list_pengadilan = [
  'pa-surabaya', 'pa-indramayu'
]

all_combined_data = []
start_time = time.time()
print(f"[{current_time()}] ▶️ Mulai scraping {len(list_pengadilan)} pengadilan...")

# Loop semua pengadilan
for pengadilan in list_pengadilan:
    links_putusan = []
    page = 1
    error_count = 0
    max_errors = 100
    print(f"\n[{current_time()}] 🔍 Scraping: {pengadilan}")

    while True:
        url = f"https://putusan3.mahkamahagung.go.id/direktori/index/pengadilan/{pengadilan}/kategori/{direktori}-1/tahunjenis/{category}/tahun/{year}/page/{page}.html"
        print(f"[{current_time()}] ➡️ Page {page}: {url}")

        try:
            session.headers.update({'User-Agent': random.choice(user_agents)})
            r = session.get(url, timeout=600)

            if r.status_code == 503:
                print(f"[{current_time()}] ⚠️ Server 503 (Service Unavailable), retrying...")
                error_count += 1
                if error_count >= max_errors:
                    print(f"[{current_time()}] ⛔ Terlalu banyak 503. Skip {pengadilan}")
                    break
                time.sleep(random.uniform(5, 10))
                continue

            if r.status_code != 200:
                print(f"[{current_time()}] ❌ Status code: {r.status_code}. Skip.")
                break

            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('.entry-c strong a')

            if not items:
                print(f"[{current_time()}] ⚠️ Tidak ditemukan item di halaman {page}")
                break

            links_putusan.extend([item['href'] for item in items])

            next_link = soup.select_one('.pagination a[rel="next"]')
            if not next_link:
                break

            page += 1
            error_count = 0
            time.sleep(random.uniform(2, 4))

        except Exception as e:
            print(f"[{current_time()}] ⚠️ Error: {e}")
            error_count += 1
            if error_count >= max_errors:
                print(f"[{current_time()}] ⛔ Terlalu banyak error. Skip {pengadilan}")
                break
            time.sleep(random.uniform(5, 10))
            continue

    print(f"[{current_time()}] 📄 Total putusan ditemukan: {len(links_putusan)}")

    for idx, link in enumerate(links_putusan):
        try:
            print(f"[{current_time()}] [{idx+1}/{len(links_putusan)}] Detail: {link}")
            session.headers.update({'User-Agent': random.choice(user_agents)})
            r = session.get(link, timeout=600)

            if r.status_code == 503:
                print(f"[{current_time()}] ⚠️ Halaman detail 503. Lewati.")
                continue

            if r.status_code != 200:
                print(f"[{current_time()}] ❌ Gagal akses detail. Status code: {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            data = {
                "url_page": link,
                "nomor": get_text_after_label(soup, 'Nomor'),
                "klasifikasi": get_text_after_label(soup, 'Klasifikasi'),
                "kata_kunci": get_text_after_label(soup, 'Kata Kunci'),
                "tanggal_register": get_text_after_label(soup, 'Tanggal Register'),
                "lembaga_peradilan": get_text_after_label(soup, 'Lembaga Peradilan'),
                "kategori": category,
                "tahun": year,
                "pengadilan": pengadilan,
                "pdf_link": get_pdf_link(soup),
            }
            all_combined_data.append(data)
            time.sleep(random.uniform(2.5, 5))
        except Exception as e:
            print(f"[{current_time()}] ⚠️ Gagal parsing detail: {e}")
            continue

output_file = f'csv/putusan_{category}_{year}_part2.csv'
if all_combined_data:
    pd.DataFrame(all_combined_data).to_csv(output_file, index=False)
    print(f"\n[{current_time()}] ✅ Selesai. Total data: {len(all_combined_data)}")
    print(f"📁 File disimpan: {output_file}")
else:
    print(f"\n[{current_time()}] ⚠️ Tidak ada data berhasil diambil.")
print(f"⏱️ Durasi: {(time.time() - start_time)/60:.2f} menit")
