"""
╔══════════════════════════════════════════════════════════════════╗
║   ZAMA — INTELLIGENT AUTONOMOUS CRAWLER v4.0                     ║
║   Chèche done kreyòl sou TOUT entènèt la poukont li              ║
║                                                                   ║
║   Estrateji:                                                      ║
║   1. Kòmanse ak seeds Google/DuckDuckGo pou jwenn sit            ║
║   2. Score chak paj (èske li vrèman kreyòl?)                     ║
║   3. Swiv lyen ki pwomèt — evite sa ki pa itil                   ║
║   4. Aprann kile pou kanpe sou yon sit                            ║
║   5. Commit otomatik sou GitHub                                   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import re
import json
import time
import random
import logging
import hashlib
import requests
import pandas as pd
import wikipediaapi
import urllib.parse

from pathlib import Path
from datetime import datetime
from collections import deque, defaultdict
from bs4 import BeautifulSoup
from tqdm import tqdm
from datasets import load_dataset

# ══════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════

OUTPUT_DIR = Path("data")
for d in ["raw", "cleaned", "logs", "state"]:
    (OUTPUT_DIR / d).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "logs" / "crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Paramèt Crawler ──
MAX_PAGES_PER_DOMAIN  = 300   # Pa twò agresif sou yon sèl sit
MAX_TOTAL_PAGES       = 50000 # Limit total pou yon run
MIN_CREOLE_SCORE      = 0.05  # Score minimòm pou konsidere yon paj
DELAY_MIN             = 1.0   # Délai minimòm ant chak paj (sekond)
DELAY_MAX             = 2.5   # Délai maximòm
SAVE_EVERY            = 200   # Sovgade chak X tèks

# ── User Agents ──
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Version/17.2 Safari/605.1.15",
]

# ══════════════════════════════════════════════
# DETEKTÈ KREYÒL — Sistèm Score Avanse
# ══════════════════════════════════════════════

# Mo kreyòl pa frekans — pi enpòtan gen plis pwen
CREOLE_HIGH_FREQ = {
    "mwen", "ou", "li", "nou", "yo", "se", "pa", "ak",
    "nan", "pou", "ki", "sa", "gen", "te", "ap", "la",
    "men", "tou", "wi", "non", "ayiti", "ayisyen", "kreyòl",
    "kreyol", "peyi", "fè", "fe", "kap", "sou"
}
CREOLE_MED_FREQ = {
    "manje", "travay", "fanmi", "pitit", "timoun", "granmoun",
    "kay", "lari", "vil", "jodi", "demen", "ane", "mwa",
    "semèn", "lòt", "premye", "plis", "mwens", "menm", "ankò",
    "deja", "toujou", "janm", "poko", "kapab", "bezwen", "vle",
    "dwe", "paske", "donk", "poutan", "yon", "youn", "bò",
    "kote", "kouman", "konsa", "anpil", "jwenn", "di", "ale",
    "vini", "pran", "bay", "wè", "zanmi", "frè", "sè",
    "manman", "papa", "depi", "pandan", "lè", "jan", "isit"
}

# Mo ki sèten 100% kreyòl (pa exists nan fransè)
CREOLE_UNIQUE = {
    "mwen", "anpil", "kounye", "poutèt", "sepandan",
    "toujou", "souvan", "kèlkeswa", "nenpòt", "ditou",
    "menm", "tèlman", "poukisa", "kifè", "kidonk",
    "alòs", "konprann", "konn", "kwè", "rele",
    "gade", "chache", "jwenn", "leve", "kouche",
    "manje", "bwè", "dòmi", "travay", "jwe"
}

def score_creole(text: str) -> float:
    """
    Retounen yon score 0.0-1.0 ki montre konbyen tèks la se kreyòl.
    Pi wo = pi kreyòl.
    """
    if not text or len(text) < 30:
        return 0.0

    words = re.findall(r'\b[a-zàâäéèêëîïôùûüçœæòì]+\b', text.lower()[:1000])
    if len(words) < 10:
        return 0.0

    score = 0.0
    total = len(words)

    for w in words:
        if w in CREOLE_UNIQUE:
            score += 3.0   # Mo eksklizif kreyòl — anpil pwen
        elif w in CREOLE_HIGH_FREQ:
            score += 2.0
        elif w in CREOLE_MED_FREQ:
            score += 1.0

    # Nòmalize
    normalized = min(score / (total * 1.5), 1.0)

    # Bonus pou karaktè espesyal kreyòl
    special_chars = len(re.findall(r'[èòàùìêôâ]', text[:500]))
    if special_chars > 5:
        normalized = min(normalized + 0.1, 1.0)

    return round(normalized, 3)

def classify_language(score: float) -> str:
    if score >= 0.25:  return "ht"
    if score >= 0.10:  return "ht_fr_mix"
    if score >= 0.05:  return "fr_with_ht"
    return "fr"

def is_worth_scraping(text: str) -> bool:
    return score_creole(text) >= MIN_CREOLE_SCORE and len(text) > 100


# ══════════════════════════════════════════════
# DISCOVERY ENGINE — Jwenn sit kreyòl otomatikman
# ══════════════════════════════════════════════

class CreoleDiscoveryEngine:
    """
    Chèche sit kreyòl sou entènèt la san liste pre-defini.
    Itilize DuckDuckGo (pa bezwen API key) pou jwenn URL.
    """

    # Rechèch otomatik pou jwenn paj kreyòl
    SEARCH_QUERIES = [
        # Rechèch jeneral kreyòl
        "site:ht",
        "kreyol ayisyen nouvèl",
        "ayiti kreyol",
        "nouvèl ayiti kreyòl",
        "haiti creole news",
        "haitian creole text",

        # Sijè espesifik an kreyòl
        "sante ayiti kreyol",
        "edikasyon ayiti kreyol",
        "agrikilti ayiti kreyol",
        "ekonomi ayiti kreyol",
        "kilti ayiti kreyol",
        "sport ayiti kreyol",
        "politik ayiti kreyol",
        "istwa ayiti kreyol",
        "relijyon ayiti kreyol",
        "teknoloji ayiti kreyol",

        # Rechèch mo-kle dirèk
        "\"mwen\" \"ou\" \"yo\" \"ayiti\"",
        "\"pou\" \"nan\" \"ki\" \"se\" \"pa\"",
        "\"ayisyen\" \"kreyol\" \"peyi\"",

        # Blòg ak sit pèsonèl
        "blog haiti kreyol",
        "wordpress haiti creole",
        "facebook haiti kreyol",

        # Dokiman ofisyèl
        "gouvernement haiti creole",
        "lwa ayiti kreyol",
        "dokiman ofisyèl kreyol",

        # Radyo ak TV
        "radyo ayiti podcast kreyol",
        "transcript emisyon kreyol",

        # Rechèch akademik
        "haitian creole corpus linguistics",
        "kreyol ayisyen rechèch",
        "haitian creole NLP dataset",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.discovered_urls = set()

    def _headers(self):
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "fr-HT, ht, fr, en-US;q=0.5",
        }

    def search_duckduckgo(self, query: str, max_results: int = 20) -> set:
        """Chèche sou DuckDuckGo HTML (pa API) — gratis."""
        urls = set()
        try:
            encoded = urllib.parse.quote(query)
            url = f"https://html.duckduckgo.com/html/?q={encoded}&kl=ht-ht"

            resp = self.session.get(url, headers=self._headers(), timeout=15)
            if resp.status_code != 200:
                return urls

            soup = BeautifulSoup(resp.text, "lxml")

            # Jwenn rezilta rechèch yo
            for result in soup.find_all("a", class_=re.compile("result__url|result__a")):
                href = result.get("href", "")
                # DuckDuckGo itilize redirect URL — ekstrè URL reyèl la
                if "uddg=" in href:
                    try:
                        real_url = urllib.parse.unquote(
                            re.findall(r'uddg=([^&]+)', href)[0]
                        )
                        urls.add(real_url)
                    except Exception:
                        pass
                elif href.startswith("http"):
                    urls.add(href)

            time.sleep(random.uniform(2.0, 4.0))  # Respecte DuckDuckGo

        except Exception as e:
            log.debug(f"DuckDuckGo erè pou '{query}': {e}")

        return urls

    def search_common_crawl_index(self, domain_hint: str = "ht") -> set:
        """
        Chèche nan Common Crawl Index — sèvè ki achive tout entènèt.
        Pa bezwen API key — piblik gratis.
        """
        urls = set()
        try:
            # Common Crawl CDX API
            api_url = (
                f"https://index.commoncrawl.org/CC-MAIN-2024-10-index"
                f"?url=*.{domain_hint}/*&output=json&limit=500&fl=url,languages"
            )
            resp = requests.get(api_url, timeout=30)
            if resp.status_code == 200:
                for line in resp.text.strip().split("\n"):
                    try:
                        data = json.loads(line)
                        url = data.get("url", "")
                        langs = data.get("languages", "")
                        # Pran URL ki posibleman kreyòl
                        if url and ("ht" in langs or ".ht" in url):
                            urls.add(url)
                    except Exception:
                        continue
                log.info(f"  Common Crawl: {len(urls)} URL jwenn pou .{domain_hint}")
        except Exception as e:
            log.warning(f"  Common Crawl erè: {e}")

        return urls

    def get_wikipedia_links(self) -> set:
        """Telechaje tout lyen soti Wikipedia kreyòl."""
        urls = set()
        try:
            wiki = wikipediaapi.Wikipedia(
                language="ht",
                extract_format=wikipediaapi.ExtractFormat.WIKI,
                user_agent="ZamaBot/4.0"
            )
            # Paj espesyal ki gen tout lyen ekstèn
            categories = ["Ayiti", "Kreyòl ayisyen", "Pòtoprens"]
            for cat in categories:
                page = wiki.page(cat)
                if page.exists():
                    for title in list(page.links.keys())[:100]:
                        p = wiki.page(title)
                        if p.exists():
                            urls.add(p.fullurl)
                    time.sleep(0.5)
        except Exception as e:
            log.warning(f"  Wikipedia discovery erè: {e}")
        return urls

    def discover_all(self) -> set:
        """Kouri tout metòd discovery ak konbine rezilta yo."""
        log.info("🔍 DISCOVERY ENGINE — Chèche sit kreyòl sou entènèt...")
        all_urls = set()

        # Metòd 1 — DuckDuckGo rechèch
        log.info("  📡 Rechèch DuckDuckGo...")
        for i, query in enumerate(tqdm(self.SEARCH_QUERIES, desc="DuckDuckGo")):
            urls = self.search_duckduckgo(query)
            all_urls.update(urls)
            if i % 5 == 0:
                log.info(f"    → {len(all_urls)} URL total jwenn")

        # Metòd 2 — Common Crawl (achiv entènèt)
        log.info("  🌐 Common Crawl index...")
        all_urls.update(self.search_common_crawl_index("ht"))
        all_urls.update(self.search_common_crawl_index("haiti"))

        # Metòd 3 — Wikipedia lyen
        log.info("  📖 Wikipedia lyen ekstèn...")
        all_urls.update(self.get_wikipedia_links())

        # Filtre URL ki pa bon
        cleaned = self._filter_urls(all_urls)
        log.info(f"  ✅ Discovery fini: {len(cleaned)} URL itil jwenn")
        return cleaned

    def _filter_urls(self, urls: set) -> set:
        """Retire URL ki klèman pa itil."""
        bad_patterns = [
            r'\.(jpg|jpeg|png|gif|pdf|zip|mp3|mp4|avi|doc|xls)$',
            r'(facebook\.com|twitter\.com|instagram\.com|youtube\.com)',
            r'(amazon\.com|ebay\.com|walmart\.com)',
            r'(google\.com/search|bing\.com/search)',
            r'(login|signup|register|cart|checkout)',
            r'#',
        ]
        bad_re = re.compile('|'.join(bad_patterns), re.I)
        return {u for u in urls if u.startswith("http") and not bad_re.search(u)}


# ══════════════════════════════════════════════
# SMART CRAWLER — Traverse paj yo entèlijamanman
# ══════════════════════════════════════════════

class SmartCrawler:
    """
    Crawler ki aprann poukont li kile yon sit vo lapenn epi kile pou kite.
    """

    def __init__(self):
        self.session = requests.Session()
        self.visited_urls    = set()
        self.visited_domains = defaultdict(int)   # domèn → kantite paj scraped
        self.failed_domains  = set()              # domèn ki bloke
        self.domain_scores   = defaultdict(list)  # domèn → [scores kreyòl]
        self.records         = []
        self.total_saved     = 0
        self.stats = {
            "pages_visited": 0,
            "pages_useful": 0,
            "pages_404": 0,
            "pages_blocked": 0,
            "domains_discovered": 0,
            "domains_abandoned": 0,
        }

        # Chaje eta anteriè si li egziste (pou kontinye apre yon entèripsyon)
        self._load_state()

    def _headers(self):
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "fr-HT, ht;q=0.9, fr;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "DNT": "1",
        }

    def _get_domain(self, url: str) -> str:
        try:
            return urllib.parse.urlparse(url).netloc.replace("www.", "")
        except Exception:
            return ""

    def _save_state(self):
        """Sovgade eta crawler pou ka reprann si entèripsyon."""
        state = {
            "visited_urls": list(self.visited_urls)[-10000:],  # Dènye 10k sèlman
            "failed_domains": list(self.failed_domains),
            "domain_scores": {k: v[-20:] for k, v in self.domain_scores.items()},
            "stats": self.stats,
            "saved_at": datetime.now().isoformat()
        }
        with open(OUTPUT_DIR / "state" / "crawler_state.json", "w") as f:
            json.dump(state, f, ensure_ascii=False)

    def _load_state(self):
        """Chaje eta anteriè pou kontinye crawling."""
        state_file = OUTPUT_DIR / "state" / "crawler_state.json"
        if state_file.exists():
            try:
                with open(state_file) as f:
                    state = json.load(f)
                self.visited_urls = set(state.get("visited_urls", []))
                self.failed_domains = set(state.get("failed_domains", []))
                self.stats = state.get("stats", self.stats)
                log.info(f"  ♻️  Eta chaje: {len(self.visited_urls)} URL deja visité")
            except Exception:
                pass

    def should_crawl_domain(self, domain: str) -> bool:
        """Deside si nou dwe kontinye sou yon domèn."""
        if domain in self.failed_domains:
            return False
        if self.visited_domains[domain] >= MAX_PAGES_PER_DOMAIN:
            return False

        # Si nou scraped 10+ paj sou yon domèn ak score mwayèn ba → abandone
        scores = self.domain_scores[domain]
        if len(scores) >= 10:
            avg_score = sum(scores) / len(scores)
            if avg_score < 0.03:
                log.info(f"  🚫 Abandone {domain} — score mwayen twò ba ({avg_score:.3f})")
                self.failed_domains.add(domain)
                self.stats["domains_abandoned"] += 1
                return False
        return True

    def fetch_page(self, url: str):
        """Telechaje yon paj ak jès entèlijan."""
        domain = self._get_domain(url)

        if not self.should_crawl_domain(domain):
            return None, None

        try:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            resp = self.session.get(url, headers=self._headers(),
                                    timeout=20, allow_redirects=True)

            if resp.status_code == 200:
                self.stats["pages_visited"] += 1
                soup = BeautifulSoup(resp.text, "lxml")
                return soup, resp.url

            elif resp.status_code == 404:
                self.stats["pages_404"] += 1
                return None, None

            elif resp.status_code in (403, 429, 503):
                self.stats["pages_blocked"] += 1
                if resp.status_code == 403:
                    self.failed_domains.add(domain)
                else:
                    time.sleep(30)  # Rate limit — tann
                return None, None

        except requests.exceptions.ConnectionError:
            self.failed_domains.add(domain)
            return None, None
        except requests.exceptions.Timeout:
            return None, None
        except Exception as e:
            log.debug(f"Fetch erè {url}: {e}")
            return None, None

    def extract_text(self, soup: BeautifulSoup) -> str:
        """Ekstrè tèks prensipal la entèlijamanman."""
        if not soup:
            return ""

        # Retire eleman ki pa itil
        for tag in soup(["script", "style", "nav", "header", "footer",
                         "aside", "form", "button", "iframe", "noscript",
                         "meta", "link", "advertisement", ".ad", ".ads",
                         ".sidebar", ".menu", ".navigation", ".comments"]):
            tag.decompose()

        # Estrateji 1 — baliz semantik
        for sel in ["article", "main", '[role="main"]',
                    ".article-content", ".post-content", ".entry-content",
                    ".article-body", ".story-body", ".news-content",
                    ".contenu", ".texte", "#content", "#article"]:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator=" ", strip=True)
                if len(text) > 200:
                    return self._clean(text)

        # Estrateji 2 — Pi gran blòk tèks
        divs = [(d, len(d.get_text(strip=True))) for d in soup.find_all("div")]
        if divs:
            best = max(divs, key=lambda x: x[1])
            if best[1] > 200:
                return self._clean(best[0].get_text(separator=" ", strip=True))

        # Estrateji 3 — Tout paragraf
        paras = [p.get_text(strip=True) for p in soup.find_all("p")
                 if len(p.get_text(strip=True)) > 40]
        if paras:
            return self._clean(" ".join(paras))

        return ""

    def _clean(self, text: str) -> str:
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'http\S+', '', text)
        text = re.sub(r'[^\w\s\'\-\.\,\!\?\:\;\«\»àâäéèêëîïôùûüçœæÀÂÄÉÈÊËÎÏÔÙÛÜÇŒÆòèùàì]', '', text)
        return text.strip()

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list:
        """Ekstrè lyen ki pi pwomèt pou kontni kreyòl."""
        if not soup:
            return []

        domain = self._get_domain(base_url)
        links = []

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            # Konplete URL relatif
            try:
                full = urllib.parse.urljoin(base_url, href)
                full = full.split("#")[0]  # Retire fragments
            except Exception:
                continue

            if not full.startswith("http"):
                continue

            # Skip fichye binè
            if re.search(r'\.(jpg|jpeg|png|gif|pdf|zip|mp3|mp4|avi|exe|dmg)$',
                         full, re.I):
                continue

            link_domain = self._get_domain(full)

            # Priyorite: lyen entèn (menm sit) vs lyen ekstèn
            if link_domain == domain:
                priority = 2  # Lyen entèn — pi bon chans gen plis kontni menm lang
            else:
                priority = 1  # Lyen ekstèn — toujou itil pou dekouvri nouvo sit

            # Bonus si tèks lyen an gen mo kreyòl
            link_text = a.get_text(strip=True).lower()
            if any(w in link_text for w in ["kreyol", "ayiti", "nouvèl", "atik"]):
                priority += 1

            links.append((priority, full))

        # Sòte pa priyorite, retounen URL yo sèlman
        links.sort(key=lambda x: -x[0])
        return [url for _, url in links]

    def process_page(self, url: str) -> dict | None:
        """Telechaje, analize, epi ekstrè yon paj konplètman."""
        if url in self.visited_urls:
            return None

        self.visited_urls.add(url)
        domain = self._get_domain(url)
        self.visited_domains[domain] += 1

        soup, final_url = self.fetch_page(url)
        if not soup:
            return None

        text = self.extract_text(soup)
        if not text:
            return None

        score = score_creole(text)
        self.domain_scores[domain].append(score)

        if score < MIN_CREOLE_SCORE:
            return None

        title = ""
        title_tag = soup.find("h1") or soup.find("title")
        if title_tag:
            title = self._clean(title_tag.get_text(strip=True))

        self.stats["pages_useful"] += 1

        return {
            "id": hashlib.md5(f"{url}{text[:50]}".encode()).hexdigest()[:12],
            "source": domain,
            "url": final_url or url,
            "title": title,
            "text": text,
            "language": classify_language(score),
            "creole_score": score,
            "char_count": len(text),
            "scraped_at": datetime.now().isoformat()
        }

    def save_records(self, force: bool = False):
        """Sovgade si ase done akimile."""
        if not self.records:
            return
        if not force and len(self.records) < SAVE_EVERY:
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = OUTPUT_DIR / "raw" / f"crawl_{ts}.jsonl"
        with open(out, "a", encoding="utf-8") as f:
            for rec in self.records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        self.total_saved += len(self.records)
        log.info(f"  💾 {len(self.records)} tèks sovgade → {out.name} "
                 f"(total: {self.total_saved})")
        self.records = []
        self._save_state()

    def crawl(self, seed_urls: set, max_pages: int = MAX_TOTAL_PAGES):
        """
        Kouri crawl la sou tout URL yo ak dekouvèt otomatik.
        Itilize yon queue priyorite pou ale sou paj ki pi pwomèt anvan.
        """
        queue = deque(seed_urls)
        self.stats["domains_discovered"] = len(
            {self._get_domain(u) for u in seed_urls}
        )

        log.info(f"🕷️  Kòmanse crawl — {len(queue)} URL nan queue")

        with tqdm(total=max_pages, desc="Crawling") as pbar:
            while queue and self.stats["pages_visited"] < max_pages:
                url = queue.popleft()

                if url in self.visited_urls:
                    continue

                rec = self.process_page(url)
                if rec:
                    self.records.append(rec)
                    pbar.set_postfix({
                        "saved": self.total_saved + len(self.records),
                        "score": f"{rec['creole_score']:.2f}",
                        "domain": rec["source"][:20]
                    })

                    # Jwenn nouvo lyen pou kontinye
                    soup, _ = self.fetch_page.__func__(self, url) if False else (None, None)

                pbar.update(1)

                # Chèche lyen nan paj la
                try:
                    soup, _ = self.fetch_page(url) if url not in self.visited_urls else (None, None)
                    if soup:
                        new_links = self.extract_links(soup, url)
                        # Ajoute sèlman lyen ki pa visité
                        for link in new_links[:20]:  # Max 20 lyen pa paj
                            if link not in self.visited_urls:
                                queue.append(link)
                        new_domains = {self._get_domain(l) for l in new_links}
                        self.stats["domains_discovered"] += len(
                            new_domains - {self._get_domain(u) for u in self.visited_urls}
                        )
                except Exception:
                    pass

                # Sovgade regilyèman
                self.save_records()

                # Afiche pwogrè chak 500 paj
                if self.stats["pages_visited"] % 500 == 0:
                    self._report_progress()

        self.save_records(force=True)
        self._report_progress()

    def _report_progress(self):
        log.info("\n" + "─"*50)
        log.info(f"📊 PWOGRÈ CRAWLER:")
        log.info(f"  Paj visité:    {self.stats['pages_visited']:,}")
        log.info(f"  Paj itil:      {self.stats['pages_useful']:,}")
        log.info(f"  404:           {self.stats['pages_404']:,}")
        log.info(f"  Bloke:         {self.stats['pages_blocked']:,}")
        log.info(f"  Domèn jwenn:   {self.stats['domains_discovered']:,}")
        log.info(f"  Domèn abandone:{self.stats['domains_abandoned']:,}")
        log.info(f"  Domèn bloke:   {len(self.failed_domains):,}")
        log.info(f"  Tèks sovgade:  {self.total_saved:,}")
        log.info("─"*50)


# ══════════════════════════════════════════════
# SEEDS — Pwen depa ki solid
# ══════════════════════════════════════════════

# URL de depa — crawler ap jwenn rès la poukont li
SEED_URLS = {
    # Wikipedia kreyòl — souri done pwòp
    "https://ht.wikipedia.org/wiki/Ayiti",
    "https://ht.wikipedia.org/wiki/Pòtoprens",
    "https://ht.wikipedia.org/wiki/Kreyòl_ayisyen",

    # Medya prensipal
    "https://www.alterpresse.org/spip.php?rubrique81",
    "https://lenouvelliste.com",
    "https://www.haitilibre.com",
    "https://rezonodwes.com",
    "https://vantbefinfo.com",
    "https://loophaiti.com",
    "https://radiotelevisioncaraibes.com",
    "https://www.rfi.fr/ht/",
    "https://www.voanouvel.com",
    "https://balistrad.com",
    "https://tripfoumi.com",
    "https://haitiliberte.com",
    "https://lenational.org",
    "https://radyoteman.com",
    "https://boukanews.com",

    # Literati ak kilti
    "https://potomitan.info/ayiti/",
    "https://espaskreyol.org",
    "https://www.tanbou.com",
    "https://woymagazine.com",

    # Done relijyon (paralèl masiv)
    "https://www.jw.org/ht/",

    # Edikasyon ak enstitisyon
    "https://akademikreyol.net",
    "https://haiti.mit.edu",

    # Blòg kreyòl
    "https://kreyoliti.wordpress.com",
    "https://pwojekreyol.blogspot.com",
}


# ══════════════════════════════════════════════
# WIKIPEDIA FULL SCRAPER
# ══════════════════════════════════════════════

class WikipediaFullScraper:
    """Telechaje TOUT Wikipedia kreyòl — dump + paj pa paj."""

    def run(self):
        log.info("📖 Wikipedia Kreyòl — download konplè...")

        # Dump
        dump_url = "https://dumps.wikimedia.org/htwiki/latest/htwiki-latest-pages-articles.xml.bz2"
        out = OUTPUT_DIR / "raw" / "wikipedia_ht_dump.xml.bz2"
        if not out.exists():
            resp = requests.get(dump_url, stream=True)
            total = int(resp.headers.get("content-length", 0))
            with open(out, "wb") as f, tqdm(total=total, unit="B", unit_scale=True,
                                             desc="Wikipedia dump") as pbar:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
            log.info(f"  ✅ Dump: {out.stat().st_size//1024//1024}MB")
        else:
            log.info("  ♻️  Wikipedia dump deja la — skip")


# ══════════════════════════════════════════════
# HUGGINGFACE DOWNLOADER
# ══════════════════════════════════════════════

class HuggingFaceDownloader:
    DATASETS = [
        ("jsbeaudry/haitian_creole_tts_11K", "hf_tts"),
        ("jsbeaudry/cmu_haitian_creole_speech", "hf_cmu"),
    ]

    def run(self):
        log.info("🤗 HuggingFace done pre-kolekte...")
        for name, out in self.DATASETS:
            try:
                ds = load_dataset(name, trust_remote_code=True)
                ds.save_to_disk(str(OUTPUT_DIR / "raw" / out))
                log.info(f"  ✅ {name}")
            except Exception as e:
                log.warning(f"  ⚠️ {name}: {e}")


# ══════════════════════════════════════════════
# NETWAYMAN FINAL
# ══════════════════════════════════════════════

class DataCleaner:

    def run(self):
        log.info("🧹 Déduplikasyon ak netwayaj final...")
        all_records, seen_ids, text_hashes = [], set(), set()

        for f in sorted((OUTPUT_DIR / "raw").glob("*.jsonl")):
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec = json.loads(line.strip())
                        rid = rec.get("id", "")
                        text = rec.get("text", "")
                        if not rid or not text or rid in seen_ids:
                            continue
                        h = hashlib.md5(text[:200].encode()).hexdigest()
                        if h in text_hashes or len(text) < 80:
                            continue
                        seen_ids.add(rid)
                        text_hashes.add(h)
                        all_records.append(rec)
                    except Exception:
                        continue

        if not all_records:
            log.warning("⚠️ Pa gen done pou netwaye!")
            return []

        log.info(f"  Total pwòp: {len(all_records):,}")

        df = pd.DataFrame(all_records)

        # Sòte pa score kreyòl (pi wo anlè)
        if "creole_score" in df.columns:
            df = df.sort_values("creole_score", ascending=False)

        df.to_csv(OUTPUT_DIR / "cleaned" / "dataset_final.csv",
                  index=False, encoding="utf-8")
        df.to_json(OUTPUT_DIR / "cleaned" / "dataset_final.jsonl",
                   orient="records", lines=True, force_ascii=False)

        # Sèlman ht + ht_fr_mix — pou fine-tuning
        ht_only = df[df["language"].isin(["ht", "ht_fr_mix"])]
        ht_only.to_json(OUTPUT_DIR / "cleaned" / "dataset_ht_only.jsonl",
                        orient="records", lines=True, force_ascii=False)

        stats = {
            "total_records": len(all_records),
            "ht_records": int((df["language"] == "ht").sum()),
            "ht_mix_records": int((df["language"] == "ht_fr_mix").sum()),
            "total_mb": round(df["text"].str.len().sum() / 1_000_000, 2),
            "avg_creole_score": round(float(df.get("creole_score", pd.Series([0])).mean()), 3),
            "by_source": df["source"].value_counts().head(20).to_dict(),
            "by_language": df["language"].value_counts().to_dict(),
            "generated_at": datetime.now().isoformat()
        }

        with open(OUTPUT_DIR / "cleaned" / "stats.json", "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        log.info("\n" + "═"*50)
        log.info("📊 REZILTA FINAL:")
        log.info(f"  Total tèks:        {stats['total_records']:,}")
        log.info(f"  Kreyòl pur (ht):   {stats['ht_records']:,}")
        log.info(f"  Melanje (ht+fr):   {stats['ht_mix_records']:,}")
        log.info(f"  Total MB:          {stats['total_mb']} MB")
        log.info(f"  Score mwayen:      {stats['avg_creole_score']}")
        log.info("═"*50)
        return all_records


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║   🇭🇹  ZAMA v4.0 — AUTONOMOUS CRAWLER  🇭🇹                        ║
║   Chèche done kreyòl sou TOUT entènèt la poukont li              ║
║   Seeds → Discovery → Smart Crawl → Deduplicate → Commit        ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    start = datetime.now()

    # ── 1. Done ki deja prèt ──
    log.info("\n╔═ ETAP 1: DONE PRÈ ═╗")
    HuggingFaceDownloader().run()
    WikipediaFullScraper().run()

    # ── 2. Discovery — jwenn sit kreyòl otomatikman ──
    log.info("\n╔═ ETAP 2: DISCOVERY ═╗")
    engine = CreoleDiscoveryEngine()
    discovered_urls = engine.discover_all()

    # Konbine seeds + URLs dekouvri
    all_seeds = SEED_URLS | discovered_urls
    log.info(f"  Total URL pou crawler: {len(all_seeds):,}")

    # ── 3. Smart Crawl ──
    log.info("\n╔═ ETAP 3: SMART CRAWL ═╗")
    crawler = SmartCrawler()
    crawler.crawl(all_seeds, max_pages=MAX_TOTAL_PAGES)

    # ── 4. Netwayaj final ──
    log.info("\n╔═ ETAP 4: NETWAYAJ ═╗")
    DataCleaner().run()

    duration = datetime.now() - start
    log.info(f"\n⏱️  Tan total: {duration}")
    print("""
╔══════════════════════════════════════════════════════════════════╗
║  ✅  FINI! Done yo ap nan: data/cleaned/                         ║
║  📊  Statistik: data/cleaned/stats.json                         ║
╚══════════════════════════════════════════════════════════════════╝
    """)

if __name__ == "__main__":
    main()
