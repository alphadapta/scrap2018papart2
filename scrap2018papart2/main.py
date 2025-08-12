import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import pytz
import time
import os
import random
import gc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# KONFIGURASI
# =========================
tz_wib = pytz.timezone('Asia/Jakarta')
def current_time():
    return datetime.now(tz_wib).strftime("%Y-%m-%d %H:%M:%S %Z")

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
]

session = requests.Session()
session.headers.update({'User-Agent': random.choice(user_agents)})

os.makedirs('csv', exist_ok=True)

def get_text_after_label(soup, label_text):
    label = soup.find('td', string=label_text)
    return label.find_next('td').text.strip() if label else 'N/A'

def get_pdf_link(soup):
    pdf_link = soup.find('a', href=True, string=lambda x: x and x.endswith('.pdf'))
    return pdf_link['href'] if pdf_link else 'N/A'

# =========================
# PARAMETER SCRAPING
# =========================
year = '2018'
category = 'regis'
direktori = 'perdata-agama'
list_pengadilan = ['pa-surabaya', 'pa-indramayu']
output_file = f'csv/putusan_{category}_{year}_part2.csv'

# =========================
# SCRAPING (Hanya jika CSV belum ada)
# =========================
if not os.path.exists(output_file):
    all_combined_data = []
    print(f"[{current_time()}] ▶️ Mulai scraping {len(list_pengadilan)} pengadilan...")

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

                if r.status_code != 200:
                    error_count += 1
                    if error_count >= max_errors:
                        break
                    time.sleep(random.uniform(5, 10))
                    continue

                soup = BeautifulSoup(r.text, 'html.parser')
                items = soup.select('.entry-c strong a')

                if not items:
                    break

                links_putusan.extend([item['href'] for item in items])

                next_link = soup.select_one('.pagination a[rel="next"]')
                if not next_link:
                    break

                page += 1
                error_count = 0
                time.sleep(random.uniform(2, 4))

            except Exception:
                error_count += 1
                if error_count >= max_errors:
                    break
                time.sleep(random.uniform(5, 10))
                continue

        print(f"[{current_time()}] 📄 Total putusan ditemukan: {len(links_putusan)}")

        for idx, link in enumerate(links_putusan):
            try:
                print(f"[{current_time()}] [{idx+1}/{len(links_putusan)}] Detail: {link}")
                session.headers.update({'User-Agent': random.choice(user_agents)})
                r = session.get(link, timeout=600)
                if r.status_code != 200:
                    error_count += 1
                    if error_count >= max_errors:
                        break
                    time.sleep(random.uniform(5, 10))
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
            except Exception:
                continue

    df = pd.DataFrame(all_combined_data)
    df.to_csv(output_file, index=False)
    print(f"\n[{current_time()}] ✅ Selesai scraping. Total data: {len(df)}")
else:
    print(f"[{current_time()}] 📂 File CSV sudah ada, skip scraping.")
    df = pd.read_csv(output_file)

# =========================
# DOWNLOAD PDF (Multi-thread + Resume)
# =========================
download_directory = 'volume_downloaded_pdf/perdata_agama/regis'
os.makedirs(download_directory, exist_ok=True)

df['nomor_asli'] = df['nomor']
df['nomor'] = df['nomor'].str.replace('/', '_', regex=False).str.replace('.', '~', regex=False)

# Filter hanya yang belum terunduh
df = df[~df['nomor'].apply(lambda x: os.path.exists(os.path.join(download_directory, f"{x}.pdf")))].reset_index(drop=True)
print(f"[{current_time()}] 📥 Total file yang akan di-download: {len(df)}")

def create_session():
    s = requests.Session()
    retries = Retry(total=1000, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["HEAD", "GET", "OPTIONS"])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def download_pdf(row):
    pdf_url = row['pdf_link']
    nomor_asli = row['nomor_asli']
    nomor = row['nomor']
    pdf_filename = os.path.join(download_directory, f'{nomor}.pdf')

    if not isinstance(pdf_url, str) or not pdf_url.strip():
        return f"[SKIP] Invalid URL | Nomor: {nomor_asli}"

    if os.path.exists(pdf_filename):
        return f"[SKIP] File exists: {pdf_filename}"

    try:
        sess = create_session()
        resp = sess.get(pdf_url, timeout=3600)
        resp.raise_for_status()
        with open(pdf_filename, 'wb') as f:
            f.write(resp.content)
        return f"[OK] {pdf_filename} | Nomor: {nomor_asli}"
    except Exception as e:
        return f"[FAILED] {pdf_url} | Nomor: {nomor_asli} | Error: {e}"
    finally:
        gc.collect()

print(f"[{current_time()}] 🚀 Mulai download PDF secara paralel...")
with ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(download_pdf, row) for _, row in df.iterrows()]
    for i, future in enumerate(as_completed(futures), 1):
        print(f"{i}/{len(df)}", future.result())

print(f"[{current_time()}] ✅ Semua PDF selesai di-download.")
