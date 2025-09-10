# src/scraper/scraper_ogol.py
import time
import hashlib
import argparse
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))


from common.logging_utils import info, ok, warn, err

URL_DEFAULT = "https://www.ogol.com.br/edicao/brasileirao-serie-a-2025/194851/melhores-desempenhos"
OUT_DEFAULT = "data/scraper/ogol_melhores_2025_full.csv"

def normalize_decimal(s):
    if not isinstance(s, str):
        return s
    t = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return s.strip()

def parse_table(html) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "DataTables_Table_0"}) \
         or soup.select_one("table.zztable.stats") \
         or soup.select_one("table.zztable")
    if table is None:
        raise RuntimeError("tabela não encontrada")

    headers = []
    for th in table.select("thead th"):
        txt = (th.get_text(strip=True) or "")
        headers.append("Jogador" if txt == "" else txt)

    rows = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        cells = []
        for i, td in enumerate(tds):
            if i == 1:
                a = td.select_one(".micrologo_and_text .text a")
                cells.append(a.get_text(strip=True) if a else td.get_text(strip=True))
            else:
                cells.append(td.get_text(strip=True))
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        rows.append(cells[:len(headers)])

    df = pd.DataFrame(rows, columns=headers)
    for col in ["P","N","M","J","MJ","MOM","G","GC","ASS","A","V","PEN","PD","PP"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_decimal)
    if "Equipe" in df.columns:
        df["Equipe"] = df["Equipe"].astype(str).str.strip()
    return df

def dismiss_overlays(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
    time.sleep(0.3)
    js_remove = """
    (function(){
      var nodes = Array.prototype.slice.call(document.querySelectorAll('div,section'));
      nodes.forEach(function(n){
        var cs = window.getComputedStyle(n);
        var zi = parseInt(cs.zIndex) || 0;
        if ((cs.position === 'fixed' || cs.position === 'sticky') && zi >= 9999) {
          n.remove();
        }
      });
      var backs = document.querySelectorAll('.modal-backdrop, .fc-ab-root, .fc-dialog-overlay');
      backs.forEach(n => n.remove());
    })();
    """
    try:
        driver.execute_script(js_remove)
    except Exception:
        pass

def get_active_page_num(driver) -> str:
    try:
        el = driver.find_element(By.CSS_SELECTOR, ".dataTables_paginate .paginate_button.current")
        return el.text.strip()
    except Exception:
        return ""

def table_signature(driver) -> str:
    try:
        tbody_html = driver.find_element(By.CSS_SELECTOR, "table.zztable tbody").get_attribute("innerHTML")
    except Exception:
        tbody_html = driver.page_source
    return hashlib.md5(tbody_html.encode("utf-8", errors="ignore")).hexdigest()

def paginate_via_js(driver) -> bool:
    script = """
    try {
      var t = (window.jQuery || window.$) && (window.jQuery || window.$)('#DataTables_Table_0').DataTable();
      if (t) { t.page('next').draw(false); return true; }
      return false;
    } catch(e) { return false; }
    """
    return bool(driver.execute_script(script))

def try_set_page_length(driver) -> bool:
    try:
        sel = driver.find_element(By.CSS_SELECTOR, "div.dataTables_length select")
        options = sel.find_elements(By.TAG_NAME, "option")
        for o in options:
            txt = (o.text or "").strip().lower()
            val = (o.get_attribute("value") or "").strip()
            if val == "-1" or txt in ("todos", "all", "100"):
                o.click()
                time.sleep(1.2)
                return True
    except Exception:
        pass
    return False

def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=opts)

def scrape_all_pages(url: str) -> pd.DataFrame:
    with build_driver(headless=True) as driver:
        info(f"abrindo {url}")
        driver.get(url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.zztable.stats, table#DataTables_Table_0"))
        )
        time.sleep(0.6)
        dismiss_overlays(driver)

        if try_set_page_length(driver):
            info("page length: ALL/100")
            return parse_table(driver.page_source)

        info("paginação via DataTables API")
        dfs = []
        seen = 0
        while True:
            html = driver.page_source
            df = parse_table(html)
            dfs.append(df)
            seen += 1
            info(f"página {seen} | linhas: {len(df)}")

            before_sig = table_signature(driver)
            before_page = get_active_page_num(driver)

            if not paginate_via_js(driver):
                warn("não foi possível avançar (API DataTables indisponível)")
                break

            try:
                WebDriverWait(driver, 10).until(
                    lambda d: table_signature(d) != before_sig or get_active_page_num(d) != before_page
                )
            except Exception:
                warn("fim da paginação detectado")
                break

            time.sleep(0.5)

        return pd.concat(dfs, ignore_index=True).drop_duplicates()

def main():
    ap = argparse.ArgumentParser(prog="scraper_ogol", description="Scraper oGol - melhores desempenhos")
    ap.add_argument("--url", default=URL_DEFAULT, help="URL da tabela do oGol")
    ap.add_argument("--out", default=OUT_DEFAULT, help="arquivo CSV de saída")
    args = ap.parse_args()

    try:
        df = scrape_all_pages(args.url)
        ok(f"total linhas: {len(df)}")
        df.to_csv(args.out, index=False, encoding="utf-8-sig")
        ok(f"salvo em {args.out}")
    except Exception as e:
        err(f"falha no scraping: {e}")
        raise

if __name__ == "__main__":
    main()
