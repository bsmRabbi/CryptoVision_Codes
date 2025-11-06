import os
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

lock = threading.Lock()

# ---- Absolute path to urls.txt and output.csv ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
URL_FILE = os.path.join(BASE_DIR, r"F:\CryptoData\Input files\BlockWorks\blockworks_urls_101-115.txt")
CSV_FILE = os.path.join(BASE_DIR, r"F:\CryptoData\Output\BlockWorks\urls_106-115_output.csv")

def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def scrape(url):
    driver = get_driver()
    driver.get(url)

    try:
        # Title
        title = driver.find_element(
            By.CSS_SELECTOR,
            "h1.self-stretch.flex-grow-0.flex-shrink-0.text-xl.md\\:text-3xl.lg\\:text-4xl.xl\\:text-5xl.font-headline.text-left.text-dark"
        ).text

        # Short Description
        short_desc = driver.find_element(
            By.CSS_SELECTOR,
            "p.flex-grow-0.flex-shrink-0.text-md.lg\\:text-lg.text-left.text-dark"
        ).text

        # Full Description
        full_desc_div = driver.find_element(
            By.CSS_SELECTOR,
            "div.prose.prose-purple.max-w-none.prose-p\\:text-justify.prose-p\\:mt-0.prose-p\\:mb-6.prose-h2\\:text-xl.prose-headings\\:scroll-mt-28.prose-headings\\:font-normal.lg\\:prose-headings\\:scroll-mt-\\[8\\.5rem\\].prose-lead\\:text-purple-500.prose-a\\:font-base.prose-a\\:border-none.prose-a\\:hover\\:border-none.prose-a\\:underline.prose-pre\\:rounded-xl.prose-pre\\:bg-purple-900.prose-pre\\:shadow-lg"
        )
        full_desc_paragraphs = [p.text for p in full_desc_div.find_elements(By.TAG_NAME, "p")]
        full_desc = "\n".join(full_desc_paragraphs)

        # Time
        time_el = driver.find_element(
            By.CSS_SELECTOR,
            "div.flex.justify-start.items-start.relative.gap-1.uppercase time"
        )
        time_text = time_el.text
        time_datetime = time_el.get_attribute("datetime")

    except Exception as e:
        print(f"[ERROR] {url} -> {e}")
        title = short_desc = full_desc = time_text = time_datetime = ""

    driver.quit()

    # Write result instantly
    with lock:
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([url, title, short_desc, full_desc, time_text, time_datetime])

    return url

def main():
    # Prepare CSV with header
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Title", "Short Description", "Full Description", "Time (text)", "Time (datetime)"])

    # Load URLs (absolute path)
    with open(URL_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    # Multithreaded scraping
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(scrape, url) for url in urls]
        for future in as_completed(futures):
            print(f"[✅ DONE] {future.result()}")

if __name__ == "__main__":
    main()

    