
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
session.headers.update({
    'User-Agent': random.choice(user_agents),
    'Accept-Language': 'id,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://putusan3.mahkamahagung.go.id/'
})

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
list_pengadilan = ['pa-karawang', 'pa-blitar', 'pa-sumedang', 'pa-tasikmalaya', 'pa-pemalang', 'pa-slawi', 'pa-kabupaten-kediri', 'pa-watampone', 'pa-lumajang', 'pa-kebumen', 'pa-jakarta-barat', 'pa-semarang']
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
                    print(f"[{current_time()}] ⚠️ Error {r.status_code} (error ke-{error_count}/{max_errors})")
                    if error_count >= max_errors:
                        print(f"[{current_time()}] ⛔ Stop scraping {pengadilan}, terlalu banyak error.")
                        break
                    time.sleep(random.uniform(5, 10))
                    continue

                # reset error jika berhasil
                error_count = 0
                soup = BeautifulSoup(r.text, 'html.parser')
                items = soup.select('.entry-c strong a')

                if not items:
                    break

                links_putusan.extend([item['href'] for item in items])

                next_link = soup.select_one('.pagination a[rel="next"]')
                if not next_link:
                    break

                page += 1
                time.sleep(random.uniform(2, 4))

            except Exception as e:
                error_count += 1
                print(f"[{current_time()}] ❌ Exception: {e} (error ke-{error_count}/{max_errors})")
                if error_count >= max_errors:
                    print(f"[{current_time()}] ⛔ Stop scraping {pengadilan}, terlalu banyak error.")
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
                    print(f"[{current_time()}] ⚠️ Detail Error {r.status_code} (error ke-{error_count}/{max_errors})")
                    if error_count >= max_errors:
                        print(f"[{current_time()}] ⛔ Stop detail scraping {pengadilan}, terlalu banyak error.")
                        break
                    time.sleep(random.uniform(5, 10))
                    continue

                # reset error jika berhasil
                error_count = 0
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
                error_count += 1
                print(f"[{current_time()}] ❌ Detail Exception: {e} (error ke-{error_count}/{max_errors})")
                if error_count >= max_errors:
                    print(f"[{current_time()}] ⛔ Stop detail scraping {pengadilan}, terlalu banyak error.")
                    break
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

# Buat folder logs
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"log_download_{datetime.now(tz_wib).strftime('%Y%m%d_%H%M%S')}.txt")

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

# Counter error global
max_errors = 100
error_count = 0

def download_pdf(row):
    global error_count
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
        if resp.status_code != 200:
            error_count += 1
            msg = f"[FAILED] HTTP {resp.status_code} | {pdf_url} | Nomor: {nomor_asli} | Error ke-{error_count}/{max_errors}"
            if error_count >= max_errors:
                msg += " ⛔ (terlalu banyak error, cek koneksi/server)"
            return msg

        with open(pdf_filename, 'wb') as f:
            f.write(resp.content)

        # Reset error count kalau berhasil
        error_count = 0
        return f"[OK] {pdf_filename} | Nomor: {nomor_asli}"

    except Exception as e:
        error_count += 1
        msg = f"[FAILED] {pdf_url} | Nomor: {nomor_asli} | Error: {e} | Error ke-{error_count}/{max_errors}"
        if error_count >= max_errors:
            msg += " ⛔ (terlalu banyak error, cek koneksi/server)"
        return msg
    finally:
        gc.collect()

print(f"[{current_time()}] 🚀 Mulai download PDF secara paralel...")

results = []
with ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(download_pdf, row) for _, row in df.iterrows()]
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        results.append(result)
        print(f"{i}/{len(df)}", result)

# =========================
# RINGKASAN HASIL
# =========================
ok_count = sum(1 for r in results if r.startswith("[OK]"))
failed_count = sum(1 for r in results if r.startswith("[FAILED]"))
skip_count = sum(1 for r in results if r.startswith("[SKIP]"))

summary = (
    f"\n[{current_time()}] 📊 Ringkasan Download:\n"
    f"   ✅ Berhasil : {ok_count}\n"
    f"   ❌ Gagal    : {failed_count}\n"
    f"   ⏭️ Skip     : {skip_count}\n"
    f"   📂 Total    : {len(results)}\n\n"
    f"[{current_time()}] ✅ Semua PDF selesai di-download.\n"
)

print(summary)

# =========================
# SIMPAN LOG KE FILE
# =========================
# simpan log di dalam folder csv/logs/
log_dir = os.path.join("csv", "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"log_download_{datetime.now(tz_wib).strftime('%Y%m%d_%H%M%S')}.txt")

with open(log_file, "w", encoding="utf-8") as f:
    f.write("=== LOG DOWNLOAD PDF ===\n")
    f.write(f"Mulai: {current_time()}\n")
    f.write(f"Total target file: {len(df)}\n\n")
    for res in results:
        f.write(res + "\n")
    f.write(summary)

print(f"[{current_time()}] 📝 Log tersimpan di: {log_file}")
