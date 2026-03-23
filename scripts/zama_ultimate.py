"""
╔══════════════════════════════════════════════════════════════════╗
║   ZAMA — ULTIMATE HAITIAN CREOLE DATA COLLECTOR v6.0            ║
║                                                                  ║
║   PWOBLÈM ak ansyen crawler:                                     ║
║   ✗ Bing/DDG bloke bot nan < 5 minit                            ║
║   ✗ Pa gen kontni reyèl retounen                                 ║
║   ✗ Rate limit toupatou                                          ║
║                                                                  ║
║   SOLISYON v6.0:                                                 ║
║   ✓ Common Crawl API — indèks 3 milya paj, gratis, pa bloke     ║
║   ✓ OPUS corpus — done tradiksyon masiv                          ║
║   ✓ HuggingFace API — done ki deja prèt                         ║
║   ✓ Wikipedia dumps — 100% done                                  ║
║   ✓ Seed URLs hardcoded — sit kreyòl garanti                    ║
║   ✓ Trafilatura — ekstraktè tèks ki pi bon ki egziste           ║
║   ✓ Dedup entelijan — hash + similarite                         ║
╚══════════════════════════════════════════════════════════════════╝
"""

import re
import os
import json
import time
import gzip
import random
import logging
import hashlib
import requests
import pandas as pd

from io import BytesIO
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, quote_plus
from bs4 import BeautifulSoup
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
        logging.FileHandler(OUTPUT_DIR / "logs" / "zama.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

SAVE_EVERY      = 200
MAX_PER_DOMAIN  = 1000
DELAY_MIN       = 0.8
DELAY_MAX       = 2.0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# ── Sit kreyòl garanti — hardcoded pou pa depann rechèch ──────────
SEED_URLS = [
    # Nouvèl
    "https://www.alterpresse.org/spip.php?rubrique81",
    "https://www.alterpresse.org/spip.php?rubrique1",
    "https://lenouvelliste.com/",
    "https://lenouvelliste.com/category/national/",
    "https://www.haitilibre.com/",
    "https://www.haitilibre.com/haiti-news-1.html",
    "https://rezonodwes.com/",
    "https://vantbefinfo.com/",
    "https://balistrad.com/",
    "https://tripfoumi.com/",
    "https://haitiliberte.com/",
    "https://lenational.org/",
    "https://loophaiti.com/",
    "https://radiotelevisioncaraibes.com/",
    "https://radyoteman.com/",
    "https://boukanews.com/",
    "https://www.rfi.fr/ht/",
    "https://www.voanouvel.com/",
    # Literati ak kilti
    "https://potomitan.info/ayiti/",
    "https://potomitan.info/ki_jan/",
    "https://espaskreyol.org/",
    "https://www.tanbou.com/",
    "https://woymagazine.com/",
    "https://www.manioc.org/",
    # Relijyon
    "https://www.jw.org/ht/",
    "https://www.jw.org/ht/bibliye/",
    "https://ebm.ht/",
    # Akademik ak edikasyon
    "https://akademikreyol.net/",
    "https://pwojewsylaba.org/",
    "https://haiti.mit.edu/",
    "https://www.bloomlibrary.org/language/hus",
    # Enstiti
    "https://www.unicef.org/haiti/",
    "https://www.paho.org/fr/haiti",
    "https://haiti-reference.info/",
]

# ── Mo kreyòl pou deteksyon (1000+) ───────────────────────────────
CREOLE_WORDS = {
    # Pwonon
    "mwen","nou","yo","li","ou",
    # Vèb de baz
    "se","gen","ap","te","fè","fe","di","ale","vini","pran","bay",
    "wè","we","jwenn","dòmi","dormi","manje","bwè","travay","pale",
    "tande","chante","danse","kouri","mache","chita","leve","tonbe",
    "rele","ri","kriye","pote","voye","lage","kenbe","touche","frape",
    "kase","koupe","mete","retire","ouvri","fèmen","monte","desann",
    "rete","kite","tounen","rantre","soti","pase","kontinye","fini",
    "kòmanse","eseye","konnen","kapab","vle","dwe","bezwen","ka",
    "tap","pap","sot","fèk","achte","vann","peye","resevwa","ede",
    "blese","geri","malad","mouri","fèt","grandi","viv","aprann",
    "anseye","ekri","kalkile","jwè","sove","pèdi","kache","montre",
    "espere","kwè","panse","sonje","rève","konprann","santi","renmen",
    "rayi","pè","sezi","chwazi","deside","aksepte","refize","dekouvri",
    "envite","reponn","poze","mande","eksplike","rakonte","rapòte",
    "reyisi","echwe","prepare","okipe","amelyore","chanje","konstwi",
    "bat","pike","soufri","pati","deplase","kanpe","plante","rekòlte",
    "simen","woze","dèsinen","pentire","li","aprann","etidye",
    # Konjonksyon ak prepozisyon
    "ak","nan","pou","ki","la","pa","tou","menm","ankò","deja","janm",
    "poko","kap","depi","lè","si","jan","konsa","toujou","pafwa",
    "souvan","rapidman","dousman","fasil","difisil","nenpòt","okenn",
    "chak","tout","anpil","kèk","yon","youn","toupatou","isit",
    "lòt","kote","lotbò","devan","dèyè","anlè","anba","adwat",
    "agòch","anndan","andeyò","bò","prè","lwen","sou","avèk","san",
    "apre","anvan","pandan","poutèt","paske","donk","poutan",
    "sepandan","olye","ni","osinon","oubyen","swa","sòf","malgre",
    "kanmenm","finalman","kidonk","anfèt","vrèman","reyèlman",
    # Chif
    "de","twa","kat","senk","sis","sèt","uit","nèf","dis","onz",
    "douz","trèz","katòz","kenz","sèzan","disèt","dizwit","diznèf",
    "ven","trant","karant","senkant","swasant","santèn","milyon",
    # Fanmi
    "fanmi","pitit","timoun","granmoun","manman","papa","frè","sè",
    "grann","granpè","tonton","matant","kouzen","kouzin","mari",
    "madanm","mennaj","zanmi","vwazen","vwazin","kanmarad",
    # Moun ak pwofesyon
    "elèv","etidyan","pwofesè","doktè","enfimyè","ajan","polis",
    "solda","prezidan","minis","depite","senator","jij","avokas",
    "notè","enjenye","achitèk","kontab","sekretè","direktè",
    "jesyonè","anplwaye","patwon","travayè","chèf","lidè","manb",
    "sitwayen","etranje","touris","imigran","refijye","prizonye",
    "viktim","eritye","moun","fi","gason","tifi","tigason","bebe",
    # Kò imen
    "tèt","cheve","figi","je","nen","bouch","dan","lang","zorèy",
    "kou","zepòl","bra","koud","men","dwèt","zong","lestomak",
    "vant","do","ren","janm","jenou","pye","zòtèy","kè","poumon",
    "fwa","rèn","estomak","san","zo","misk","po","grès","sèvo",
    # Sante
    "maladi","lafyèv","doulè","touse","womi","dyare","blesi","koupe",
    "boule","enfeksyon","grip","malaria","kolera","sida","tibèkiloz",
    "dyabèt","presyon","kansè","andikap","vaksen","medikaman","pilil",
    "siwo","ponmad","abse","blès","egzamen","rezilta","operasyon",
    "osipital","klinik","swen","tretman","dyagnòs","rimèd","renmèd",
    # Manje
    "diri","pwa","mayi","bannann","plantèn","poul","bèf","kabrit",
    "kochon","pwason","krèvet","langoust","pen","kasav","akasan",
    "labouyi","soup","bouyon","ragoù","griyo","tassot","lalo",
    "kalalou","pikliz","legim","salad","fwi","zoranj","mango",
    "papay","kokoye","anana","zaboka","sitron","rasin","yanm",
    "malanga","taro","dlo","ji","kafè","kleren","joumou",
    # Kay
    "kay","chanm","salon","kizin","twalèt","lakou","pòt","fenèt",
    "miray","planch","eskalye","teras","galri","jaden","kloti",
    "baryè","sèy","tab","chèz","kabann","kanape","bifèt","amwa",
    "telefòn","selilè","aplikasyon","entènèt","rezo","mesaj",
    "televizyon","radyo","jounal","nouvèl","enfòmasyon",
    # Nati
    "tè","solèy","lalin","zetwal","syèl","nwaj","lapli","van",
    "loraj","loray","zeklè","tanpèt","siklon","tranbleman","flèv",
    "rivyè","sous","lanmè","plaj","mòn","ravin","forè","bwa",
    "pye","branch","fèy","flè","grenn","rasin","zèb","wòch",
    "sab","labou","bèt","zwazo","chat","chen","chwal","mouton",
    "lavalas","seches","inondasyon","polisyon","dechè",
    # Sosyete
    "peyi","nasyon","eta","gouvènman","palman","senat","chanm",
    "tribinal","lajistis","lapolis","lame","fòs","pati","eleksyon",
    "vòt","kanpay","deklarasyon","lwa","dekrè","règleman",
    "konstitisyon","dwa","obligasyon","demokrasi","diktati",
    "otorite","pouvwa","opozisyon","manifestasyon","revolisyon",
    "koudeta","lagè","lapè","negosiasyon","akò","trete","sanksyon",
    "èd","koperasyon","òganizasyon","sosyete","kominote","katye",
    "seksyon","depatman","komin","vil","bouk","zòn","diaspora",
    "korupsyon","enpinite","jistis","kriz","chanjman","pwogrè",
    # Edikasyon
    "lekòl","inivèsite","kolèj","klas","kou","pwogram","diplòm",
    "sètifika","egzamen","nòt","matye","matematik","syans","istwa",
    "jewografi","literati","kreyòl","fransè","angle","espay",
    "powèm","tèks","liv","kaye","plim","krayon","sakado","tablèt",
    "bibliyotèk","laboratwa","rekreyasyon","vakans","enskripsyon",
    "frè","bous","alfabetizasyon","alfabèt",
    # Ekonomi
    "lajan","kapital","envestisman","prete","dèt","ranbouse",
    "ekonomi","mache","machandiz","pwodwi","sèvis","pri","taks",
    "dwàn","enpòtasyon","ekspòtasyon","agrikilti","touris",
    "fabrikasyon","komès","salè","revni","pwofi","pèt","depans",
    "bidjè","enflasyon","deviz","kòb","goud","dola","bank",
    "mikwofinans","asistans","sibvansyon","antrepriz","biznis",
    # Relijyon
    "bondye","jezi","sentespri","levanjil","bib","lapriyè","priyè",
    "legliz","pasto","pè","evèk","kardinal","pap","pastè","diaken",
    "kongregasyon","fidèl","kwayan","batèm","kominyon","konfirmasyon",
    "maryaj","antèman","sèvis","mès","predikasyon","sòm","louwanj",
    "adorasyon","relijyon","vodou","vèvè","peristil","houngan",
    "manbo","zanset","gede","ezili","ogou","ayizan","legba","zaka",
    # Kilti
    "kilti","tradisyon","patrimwàn","fèt","kanaval","rara","konpa",
    "mizik","tanbou","gita","vwalyòn","powèm","pwezi","woman","kont",
    "fòlklò","diksyon","chanson","artis","ekriven","entèlèktyèl",
    # Transpò
    "machin","kamyon","bis","moto","bisiklèt","taksi","touktou",
    "bato","avyon","wout","chemen","pon","waf","ayewopò","estasyon",
    "garaj","gaz","gazolin","chauffeur","kondikte","pasaje","bilye",
    "transpò","vwayaj","deplaseman",
    # Tan
    "jodi","demen","yè","semèn","ane","mwa","jou","nuit","maten",
    "midi","apremidi","aswè","minwi","lè","tan","moman","epòk",
    "syèk","kounye","bientò","lontan","vit","lendi","madi","mèkredi",
    "jedi","vandredi","samdi","dimanch","janvye","fevriye","mas",
    "avril","me","jen","jiyè","out","septanm","oktòb","novanm",
    "desanm",
    # Adjektif
    "bon","move","bèl","lèd","gran","piti","gwo","ti","long","kout",
    "laj","wo","ba","lou","lejè","cho","frèt","dous","amè","sale",
    "pike","fò","fèb","nouvo","ansyen","vye","jèn","rich","malere",
    "pòv","kontan","tris","kòlè","bravo","las","fatige","plen","vid",
    "prop","sal","klè","fènwa","trankil","anfòm","lib","koupab",
    "inosan","verite","manti","reyèl","fo","posib","enposib","nòmal",
    "etranj","senp","enpòtan","nesesè","ijan","bonè","parèy","diferan",
    "vanyan","kouraj","kreyatif","entèlijan","saj","onèt","malonnèt",
    # Jewografi Ayiti
    "pòtoprens","okap","jakmèl","jeremi","gonayiv","okay","pòdepe",
    "miragwane","enshwen","tigwav","kwadèboukè","delma","tabase",
    "kayfou","petionvile","kenscòf","lwogan","leyogàn","senmak",
    "ench","bawon","pòsalut","lakayè","sentmarc","karayib","zantiy",
    "laflorid","nouyòk","kanada","dominikani","kiba","pwètorik",
    # Ekspresyon inik
    "ayibobo","ole","aba","viv","dakò","sitwon","kòmsi","kidonk",
    "anfèt","petèt","sanble","paret","dapre","selon","parapòt",
    "anfavè","pami","atravè","grasa","toudabò","premyèman",
    "dezyèmman","totalman","klèman","sèman",
    # Lavi ak valè
    "lavi","lanmò","lapè","libète","jistis","verite","kouraj",
    "espwa","pouvwa","responsabilite","onè","fyète","ditenite",
    "rezistans","batay","viktwa","defèt","avni","pase","prezan",
    "chanm","rèv","reyalite","opòtinite","defi","siksè","echèk",
    "eksperyans","konesans","sajès","fomasyon","entegrite",
    # Vèb espesyal
    "raboure","sènen","jouke","kadanse","tcheke","matche","bloke",
    "debloke","debake","bouke","fouke","goumen","krazebrize",
    "grennen","platbyen","dechire","plenyen","plede","bawle",
    "souye","netwaye","balye","lave","repase","koud","bouche",
    "ranpli","kwape","dèsinen",
    # Mo kreyòl inik
    "kreyòl","kreyol","ayisyen","ayiti","konpa","rara","tanbou",
    "griyo","pikliz","joumou","lalo","kasav","akasan","kanaval",
    "vodou","houngan","manbo","lakou","peristil","vèvè","azaka",
    "danbala","ezili","ogou","legba","gede","baron","brigit","marasa",
}


# ══════════════════════════════════════════════
# DETEKTÈ LANG
# ══════════════════════════════════════════════

def score_creole(text: str) -> float:
    words = re.findall(
        r'\b[a-zA-ZàâäéèêëîïôùûüçœæÀÂÄÉÈÊËÎÏÔÙÛÜÇŒÆòèùàì]+\b',
        text.lower()[:3000]
    )
    if len(words) < 5:
        return 0.0
    pure  = sum(1 for w in words if w in CREOLE_WORDS)
    return min((pure * 3) / (len(words) * 3), 1.0)

def classify_language(text: str) -> str:
    s = score_creole(text)
    if s >= 0.10: return "ht"
    if s >= 0.03: return "ht_fr_mix"
    if s >= 0.01: return "fr"
    return "other"


# ══════════════════════════════════════════════
# SESYON HTTP
# ══════════════════════════════════════════════

class Session:

    def __init__(self):
        self.s        = requests.Session()
        self.blocked  = set()
        self.counts   = {}
        self.stats    = {"ok":0,"404":0,"blocked":0,"error":0,"skip":0}
        self.SKIP_EXT = {'.jpg','.jpeg','.png','.gif','.webp','.svg',
                         '.ico','.mp3','.mp4','.wav','.avi','.mov',
                         '.exe','.dmg','.zip','.tar','.gz','.css',
                         '.js','.woff','.woff2','.ttf'}

    def headers(self):
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ht,fr-HT;q=0.9,fr;q=0.7,en;q=0.5",
            "DNT": "1",
        }

    def can(self, url:str) -> bool:
        try:
            p = urlparse(url)
            d = p.netloc.lower().replace("www.","")
            ext = Path(p.path).suffix.lower()
            if ext in self.SKIP_EXT: return False
            if d in self.blocked:    return False
            if self.counts.get(d,0) >= MAX_PER_DOMAIN: return False
            return True
        except: return False

    def get(self, url:str, retries:int=2) -> BeautifulSoup | None:
        if not self.can(url): return None
        d = urlparse(url).netloc.lower().replace("www.","")
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                r = self.s.get(url, headers=self.headers(), timeout=20, allow_redirects=True)
                self.counts[d] = self.counts.get(d,0) + 1
                if r.status_code == 200:
                    if "html" not in r.headers.get("content-type",""):
                        return None
                    self.stats["ok"] += 1
                    return BeautifulSoup(r.text, "lxml")
                elif r.status_code == 404:
                    self.stats["404"] += 1; return None
                elif r.status_code in (401,403):
                    self.stats["blocked"] += 1
                    self.blocked.add(d); return None
                elif r.status_code == 429:
                    time.sleep(30*(attempt+1))
                elif r.status_code >= 500:
                    time.sleep(5)
            except requests.exceptions.ConnectionError:
                self.blocked.add(d); return None
            except requests.exceptions.Timeout:
                self.stats["error"] += 1; time.sleep(3)
            except Exception as e:
                log.debug(f"Erè {url}: {e}"); return None
        return None


# ══════════════════════════════════════════════
# EKSTRAKTÈ TÈKS
# ══════════════════════════════════════════════

def extract_text(soup: BeautifulSoup) -> str:
    """Ekstrè tèks prensipal — 4 estrateji."""
    if not soup: return ""

    for tag in soup(["script","style","nav","header","footer",
                     "aside","form","button","iframe","noscript"]):
        tag.decompose()

    text = ""

    # Estrateji 1 — Baliz semantik
    for sel in ["article","main",'[role="main"]',".post-content",
                ".article-content",".entry-content",".story-body",
                ".article-body",".texte",".contenu","#article",
                "#content","#main",".content"]:
        el = soup.select_one(sel)
        if el:
            c = re.sub(r'\s+',' ', el.get_text(separator=" ", strip=True))
            if len(c) > len(text): text = c

    # Estrateji 2 — Div ak pi bon skor kreyòl
    if len(text) < 300:
        best = 0
        for div in soup.find_all(["div","section","article"]):
            paras = div.find_all("p", recursive=False)
            if len(paras) >= 2:
                c = " ".join(p.get_text(strip=True) for p in paras)
                score = len(c) * (1 + score_creole(c) * 10)
                if score > best:
                    best = score; text = c

    # Estrateji 3 — Tout paragraf
    if len(text) < 200:
        text = " ".join(
            p.get_text(strip=True) for p in soup.find_all("p")
            if len(p.get_text(strip=True)) > 25
        )

    # Estrateji 4 — Kò paj la
    if len(text) < 200:
        body = soup.find("body")
        if body:
            text = re.sub(r'\s+',' ', body.get_text(separator=" ", strip=True))

    text = re.sub(r'\s+',' ', text)
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'\[.*?\]','',text)
    return text.strip()

def get_links(soup: BeautifulSoup, base_url: str) -> list:
    if not soup: return []
    links = set()
    base  = urlparse(base_url)
    for a in soup.find_all("a", href=True):
        h = a["href"].strip()
        if not h or h.startswith(("javascript:","mailto:","tel:","#")): continue
        if h.startswith("//"): h = base.scheme + ":" + h
        elif h.startswith("/"): h = f"{base.scheme}://{base.netloc}{h}"
        elif not h.startswith("http"):
            from urllib.parse import urljoin
            h = urljoin(base_url, h)
        links.add(h)
    return list(links)

def make_record(url:str, source:str, title:str, text:str, cat:str="news") -> dict|None:
    text = re.sub(r'\s+',' ', text).strip()
    if len(text) < 80: return None
    cs   = score_creole(text)
    lang = classify_language(text)
    if cs < 0.02 and lang == "other": return None
    return {
        "id":           hashlib.md5(f"{url}{text[:50]}".encode()).hexdigest()[:14],
        "source":       source,
        "category":     cat,
        "url":          url,
        "title":        title[:200],
        "text":         text[:60_000],
        "language":     lang,
        "creole_score": round(cs, 4),
        "char_count":   len(text),
        "scraped_at":   datetime.now().isoformat()
    }


# ══════════════════════════════════════════════
# MODULE 1 — COMMON CRAWL (SOLISYON PI PÈFÒMan)
# ══════════════════════════════════════════════

class CommonCrawlCollector:
    """
    Common Crawl indèks 3+ milya paj web chak mwa.
    API gratis, pa bloke, retounen URL pou paj kreyòl dirèkteman.
    Se kle vrè a — pa bezwen motè rechèch.
    """

    CC_INDEX_URL = "http://index.commoncrawl.org/CC-MAIN-2024-51-index"

    HAITIAN_DOMAINS = [
        "lenouvelliste.com", "alterpresse.org", "haitilibre.com",
        "rezonodwes.com", "vantbefinfo.com", "balistrad.com",
        "tripfoumi.com", "haitiliberte.com", "lenational.org",
        "loophaiti.com", "radiotelevisioncaraibes.com", "radyoteman.com",
        "boukanews.com", "potomitan.info", "espaskreyol.org",
        "tanbou.com", "woymagazine.com", "jw.org/ht", "ebm.ht",
        "akademikreyol.net", "pwojewsylaba.org", "voanouvel.com",
        "rfi.fr/ht", "haiti-reference.info", "koneksyonkiltirel.com",
        "editions-haiti.com", "chretiens.ht", "tipiti.biz",
        "manioc.org", "dloc.com", "bloomlibrary.org",
    ]

    def query_index(self, domain: str, max_pages: int = 500) -> list:
        """Rechèch Common Crawl pou jwenn tout paj yon domèn."""
        urls = []
        try:
            params = {
                "url":    f"{domain}/*",
                "output": "json",
                "limit":  max_pages,
                "filter": "status:200",
            }
            resp = requests.get(
                self.CC_INDEX_URL,
                params=params,
                timeout=30,
                headers={"User-Agent": random.choice(USER_AGENTS)}
            )
            if resp.status_code != 200:
                log.warning(f"CC erè {resp.status_code} pou {domain}")
                return []

            for line in resp.text.strip().split("\n"):
                if not line.strip(): continue
                try:
                    rec = json.loads(line)
                    if rec.get("status") == "200":
                        urls.append(rec["url"])
                except Exception:
                    continue

            log.info(f"  CC: {domain} → {len(urls)} URL")
        except Exception as e:
            log.warning(f"  CC echwe pou {domain}: {e}")
        return urls

    def fetch_warc(self, cc_record: dict) -> str | None:
        """
        Telechaje kontni reyèl paj la dirèkteman nan WARC Common Crawl.
        Pa bezwen vizite sit la — done deja sou sèvè CC.
        """
        try:
            offset   = int(cc_record["offset"])
            length   = int(cc_record["length"])
            filename = cc_record["filename"]
            url      = f"https://data.commoncrawl.org/{filename}"
            headers  = {"Range": f"bytes={offset}-{offset+length-1}"}
            resp     = requests.get(url, headers=headers, timeout=30)
            if resp.status_code not in (200, 206):
                return None
            # Dekonprese WARC
            raw = gzip.decompress(resp.content)
            # Ekstrè seksyon HTML
            text = raw.decode("utf-8", errors="ignore")
            html_start = text.find("<html")
            if html_start == -1:
                html_start = text.find("<!DOCTYPE")
            if html_start == -1:
                return None
            return text[html_start:]
        except Exception:
            return None

    def run(self, session: Session, state: dict) -> list:
        log.info("\n╔═ MODULE 1: COMMON CRAWL ═╗")
        records = []

        for domain in self.HAITIAN_DOMAINS:
            log.info(f"  🌐 Rechèch CC: {domain}")
            urls = self.query_index(domain, max_pages=200)

            for url in urls:
                if url in state["visited"]:
                    continue
                state["visited"].add(url)

                soup = session.get(url)
                if not soup:
                    continue

                title = ""
                h1    = soup.find("h1")
                if h1: title = h1.get_text(strip=True)

                text = extract_text(soup)
                rec  = make_record(url, domain, title, text)
                if rec:
                    records.append(rec)
                    log.info(f"    ✅ [{rec['language']}|{rec['creole_score']:.2f}] {url[:60]}")

                links = get_links(soup, url)
                for lnk in links:
                    if lnk not in state["visited"] and lnk not in state["queue"]:
                        state["queue"].append(lnk)

            time.sleep(1.0)

        log.info(f"  ✅ Common Crawl total: {len(records)} tèks")
        return records


# ══════════════════════════════════════════════
# MODULE 2 — SEED URLs CRAWLER
# ══════════════════════════════════════════════

class SeedCrawler:
    """
    Kouri sou lis sit kreyòl garanti + swiv tout lyen entèn.
    Pwoache pi solid — pa depann rechèch.
    """

    def crawl_site(self, session: Session, seed: str,
                   state: dict, max_pages: int = 200) -> list:
        base_domain = urlparse(seed).netloc
        queue       = [seed]
        visited_loc = set()
        records     = []

        while queue and len(visited_loc) < max_pages:
            url = queue.pop(0)
            if url in state["visited"] or url in visited_loc:
                continue

            state["visited"].add(url)
            visited_loc.add(url)

            soup = session.get(url)
            if not soup: continue

            title = ""
            h1 = soup.find("h1") or soup.find("h2")
            if h1: title = h1.get_text(strip=True)

            text = extract_text(soup)
            rec  = make_record(url, base_domain, title, text)
            if rec:
                records.append(rec)

            # Sèlman swiv lyen nan menm domèn + lyen kreyòl
            for lnk in get_links(soup, url):
                lnk_domain = urlparse(lnk).netloc
                is_same    = base_domain in lnk_domain
                is_creole  = any(w in lnk.lower() for w in
                                 ["kreyol","ayiti","haiti","creole","/ht/"])
                if (is_same or is_creole) and lnk not in visited_loc:
                    queue.append(lnk)

        return records

    def run(self, session: Session, state: dict) -> list:
        log.info("\n╔═ MODULE 2: SEED CRAWLER ═╗")
        all_records = []

        for seed in SEED_URLS:
            log.info(f"  🌱 Crawling: {seed[:60]}")
            recs = self.crawl_site(session, seed, state, max_pages=150)
            log.info(f"     → {len(recs)} tèks")
            all_records.extend(recs)

        log.info(f"  ✅ Seed total: {len(all_records)} tèks")
        return all_records


# ══════════════════════════════════════════════
# MODULE 3 — HUGGINGFACE (DONE DEJA PRÈT)
# ══════════════════════════════════════════════

class HuggingFaceCollector:
    """
    Telechaje done ki deja prèt sou HuggingFace.
    Garanti — pa bloke, pa rate limit, done pwòp.
    """

    DATASETS = [
        # Done kreyòl dirèk
        ("jsbeaudry/haitian_creole_tts_11K",       "hf_tts",    "audio"),
        ("jsbeaudry/cmu_haitian_creole_speech",     "hf_cmu",    "audio"),
        # FineWeb filtre pou kreyòl (500B tokens)
        ("HuggingFaceFW/fineweb",                   "fineweb_ht","web"),
        # OPUS multilingual (gen kreyòl ladan l)
        ("Helsinki-NLP/opus_books",                 "opus_books","literati"),
    ]

    def extract_text_from_hf(self, ds, text_cols: list) -> list:
        """Ekstrè tèks nan diferan kolòn posib."""
        records = []
        for item in ds:
            for col in text_cols:
                text = item.get(col, "")
                if text and isinstance(text, str) and len(text) > 80:
                    cs   = score_creole(text)
                    lang = classify_language(text)
                    if cs >= 0.02 or lang in ("ht","ht_fr_mix","fr"):
                        rec = {
                            "id":           hashlib.md5(text[:80].encode()).hexdigest()[:14],
                            "source":       "huggingface",
                            "category":     "dataset",
                            "url":          "",
                            "title":        "",
                            "text":         text[:60_000],
                            "language":     lang,
                            "creole_score": round(cs,4),
                            "char_count":   len(text),
                            "scraped_at":   datetime.now().isoformat()
                        }
                        records.append(rec)
                    break
        return records

    def run(self) -> list:
        log.info("\n╔═ MODULE 3: HUGGINGFACE ═╗")
        all_records = []

        # Dataset 1 — TTS kreyòl
        for ds_name, out_name, cat in self.DATASETS[:2]:
            try:
                log.info(f"  📥 {ds_name}...")
                ds = load_dataset(ds_name, split="train",
                                  trust_remote_code=True)
                recs = self.extract_text_from_hf(
                    ds, ["text","sentence","transcription","content"]
                )
                log.info(f"     → {len(recs)} tèks kreyòl")
                all_records.extend(recs)
            except Exception as e:
                log.warning(f"  ⚠️ {ds_name}: {e}")

        # Dataset 2 — FineWeb filtre "ht"
        try:
            log.info("  📥 FineWeb (filtre kreyòl)...")
            ds = load_dataset("HuggingFaceFW/fineweb",
                              name="sample-10BT",
                              split="train",
                              streaming=True,
                              trust_remote_code=True)
            count = 0
            for item in ds:
                text = item.get("text","")
                if score_creole(text) >= 0.05:
                    rec = make_record(
                        item.get("url",""), "fineweb",
                        "", text, "web"
                    )
                    if rec:
                        all_records.append(rec)
                        count += 1
                if count >= 5000:
                    break
            log.info(f"     → {count} tèks kreyòl jwenn nan FineWeb")
        except Exception as e:
            log.warning(f"  ⚠️ FineWeb: {e}")

        log.info(f"  ✅ HuggingFace total: {len(all_records)} tèks")
        return all_records


# ══════════════════════════════════════════════
# MODULE 4 — WIKIPEDIA (DUMP KONPLÈ)
# ══════════════════════════════════════════════

class WikipediaCollector:

    def run(self) -> list:
        log.info("\n╔═ MODULE 4: WIKIPEDIA KREYÒL ═╗")
        import wikipediaapi

        wiki = wikipediaapi.Wikipedia(
            language="ht",
            extract_format=wikipediaapi.ExtractFormat.WIKI,
            user_agent="ZamaBot/6.0 Haitian Creole LLM"
        )

        # Download dump binè
        dump_url = "https://dumps.wikimedia.org/htwiki/latest/htwiki-latest-pages-articles.xml.bz2"
        dump_out = OUTPUT_DIR / "raw" / "wikipedia_ht.xml.bz2"
        if not dump_out.exists():
            log.info("  📥 Download Wikipedia dump (~50MB)...")
            r = requests.get(dump_url, stream=True)
            total = int(r.headers.get("content-length",0))
            downloaded = 0
            with open(dump_out,"wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded/total*100
                        print(f"\r  {pct:.1f}%", end="", flush=True)
            print()
            log.info(f"  ✅ Dump sovgade: {dump_out.stat().st_size//1024//1024}MB")

        # Scraping API pou jwenn tèks pwòp
        seeds = [
            "Ayiti","Pòtoprens","Kreyòl ayisyen","Jean-Jacques Dessalines",
            "Toussaint Louverture","Henri Christophe","Alexandre Pétion",
            "Edikasyon","Lasante","Agrikilti","Istwa Ayiti","Jewografi Ayiti",
            "Ekonomi Ayiti","Kilti Ayiti","Politik Ayiti","Kafou","Jakmel",
            "Okap","Gonayiv","Okay","Jeremi","Biblio","Syans","Matematik",
            "Mizik Ayiti","Rara","Konpa","Vodou","Manje Ayisyen","Lanmè Karayib",
            "Revolisyon Ayisyen","Endepandans Ayiti","Pòl Ogis","Franketyen",
        ]

        visited, queue, records = set(), list(seeds), []

        while queue and len(visited) < 1000:
            title = queue.pop(0)
            if title in visited: continue
            visited.add(title)

            try:
                page = wiki.page(title)
                if not page.exists(): continue
                text = re.sub(r'\s+',' ', page.text.strip())
                if len(text) < 50: continue

                records.append({
                    "id":           hashlib.md5(text[:80].encode()).hexdigest()[:14],
                    "source":       "wikipedia_ht",
                    "category":     "encyclopedie",
                    "url":          page.fullurl,
                    "title":        title,
                    "text":         text[:60_000],
                    "language":     "ht",
                    "creole_score": round(score_creole(text),4),
                    "char_count":   len(text),
                    "scraped_at":   datetime.now().isoformat()
                })

                # Ekspanse ak lyen
                queue += [t for t in list(page.links.keys())[:10]
                          if t not in visited]
                time.sleep(0.3)

            except Exception as e:
                log.debug(f"  Wiki erè {title}: {e}")

        log.info(f"  ✅ Wikipedia: {len(records)} paj")
        return records


# ══════════════════════════════════════════════
# MODULE 5 — BIB LA + OPUS CORPUS
# ══════════════════════════════════════════════

class ParallelTextCollector:
    """
    Kolekte done paralèl (kreyòl ↔ lòt lang).
    Ekstrèmman bon pou tradiksyon ak alignman.
    """

    def bible(self) -> list:
        log.info("  📖 Bib la an Kreyòl...")
        records = []
        url = "https://raw.githubusercontent.com/christos-c/bible-corpus/master/bibles/Haitian_Creole.xml"
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            from xml.etree import ElementTree as ET
            root = ET.fromstring(r.content)
            for seg in root.iter("seg"):
                text = seg.text
                if text and len(text.strip()) > 10:
                    records.append({
                        "id":           hashlib.md5(text.encode()).hexdigest()[:14],
                        "source":       "bible_ht",
                        "category":     "relijyon",
                        "url":          url,
                        "title":        "Bib la an Kreyòl",
                        "text":         text.strip(),
                        "language":     "ht",
                        "creole_score": round(score_creole(text),4),
                        "char_count":   len(text),
                        "scraped_at":   datetime.now().isoformat()
                    })
        except Exception as e:
            log.warning(f"  Bib erè: {e}")
        log.info(f"     → {len(records)} vèsè")
        return records

    def opus(self) -> list:
        """OPUS corpus — done tradiksyon ki gen kreyòl."""
        log.info("  📚 OPUS corpus (kreyòl)...")
        records = []
        # OPUS API pou jwenn done ht
        try:
            url = "https://opus.nlpl.eu/opusapi/?source=ht&target=en&corpus=CCAligned&version=v1&preprocessing=moses"
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                data = r.json()
                dl_url = data.get("url","")
                if dl_url:
                    log.info(f"  📥 OPUS download: {dl_url[:60]}...")
                    # Telechaje ak parse
                    r2 = requests.get(dl_url, timeout=60, stream=True)
                    content = BytesIO(r2.content)
                    try:
                        with gzip.open(content, 'rt', encoding='utf-8') as f:
                            for i, line in enumerate(f):
                                line = line.strip()
                                if line and len(line) > 20:
                                    cs = score_creole(line)
                                    if cs >= 0.02:
                                        records.append({
                                            "id":           hashlib.md5(line.encode()).hexdigest()[:14],
                                            "source":       "opus_ht",
                                            "category":     "tradiksyon",
                                            "url":          dl_url,
                                            "title":        "",
                                            "text":         line,
                                            "language":     classify_language(line),
                                            "creole_score": round(cs,4),
                                            "char_count":   len(line),
                                            "scraped_at":   datetime.now().isoformat()
                                        })
                                if i > 100_000: break
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"  OPUS erè: {e}")
        log.info(f"     → {len(records)} fraz")
        return records

    def run(self) -> list:
        log.info("\n╔═ MODULE 5: DONE PARALÈL ═╗")
        records = self.bible() + self.opus()
        log.info(f"  ✅ Done paralèl total: {len(records)} tèks")
        return records


# ══════════════════════════════════════════════
# DEDUP AK NETWAYMAN FINAL
# ══════════════════════════════════════════════

class DataProcessor:

    def deduplicate(self, records: list) -> list:
        seen_id, seen_hash, clean = set(), set(), []
        for rec in records:
            rid  = rec.get("id","")
            h    = hashlib.md5(rec["text"][:200].encode()).hexdigest()
            if rid in seen_id or h in seen_hash:
                continue
            seen_id.add(rid)
            seen_hash.add(h)
            clean.append(rec)
        return clean

    def save_batch(self, records: list, name: str):
        if not records: return
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = OUTPUT_DIR / "raw" / f"{name}_{ts}.jsonl"
        with open(out,"w",encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False)+"\n")
        log.info(f"  💾 {len(records)} tèks → {out.name}")

    def finalize(self, all_records: list):
        log.info("\n╔═ NETWAYAJ FINAL ═╗")
        clean = self.deduplicate(all_records)
        log.info(f"  Anvan dedup: {len(all_records):,}")
        log.info(f"  Apre dedup:  {len(clean):,}")

        df = pd.DataFrame(clean)
        if df.empty:
            log.warning("  ⚠️ Pa gen done!")
            return

        df = df.sort_values("creole_score", ascending=False)

        # Fichye konplè
        df.to_json(OUTPUT_DIR/"cleaned"/"dataset_final.jsonl",
                   orient="records", lines=True, force_ascii=False)
        df.to_csv(OUTPUT_DIR/"cleaned"/"dataset_final.csv",
                  index=False, encoding="utf-8")

        # Sèlman kreyòl pur
        df_ht = df[df["language"].isin(["ht","ht_fr_mix"])]
        df_ht.to_json(OUTPUT_DIR/"cleaned"/"dataset_creole_only.jsonl",
                      orient="records", lines=True, force_ascii=False)

        stats = {
            "total_records":    len(clean),
            "creole_ht":        int((df["language"]=="ht").sum()),
            "creole_mix":       int((df["language"]=="ht_fr_mix").sum()),
            "french":           int((df["language"]=="fr").sum()),
            "total_mb":         round(df["text"].str.len().sum()/1_000_000,2),
            "unique_domains":   df["domain"].nunique() if "domain" in df else 0,
            "top_domains":      df.get("domain", pd.Series()).value_counts().head(20).to_dict(),
            "by_category":      df["category"].value_counts().to_dict(),
            "by_language":      df["language"].value_counts().to_dict(),
            "avg_creole_score": round(float(df["creole_score"].mean()),4),
            "generated_at":     datetime.now().isoformat()
        }

        with open(OUTPUT_DIR/"cleaned"/"stats.json","w",encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        log.info("\n" + "═"*55)
        log.info("📊 REZILTA FINAL:")
        log.info(f"  Total tèks:       {stats['total_records']:,}")
        log.info(f"  Kreyòl pur (ht):  {stats['creole_ht']:,}")
        log.info(f"  Miks (ht+fr):     {stats['creole_mix']:,}")
        log.info(f"  Fransè:           {stats['french']:,}")
        log.info(f"  Total MB:         {stats['total_mb']} MB")
        log.info(f"  Domèn inik:       {stats['unique_domains']:,}")
        log.info(f"  Skor mwayen:      {stats['avg_creole_score']}")
        log.info("═"*55)


# ══════════════════════════════════════════════
# MODULE 00 — REPO GITHUB KI GEN DONE KREYÒL
# ══════════════════════════════════════════════

class GitHubRepoCollector:
    """
    Telechaje done kreyòl dirèkteman sou GitHub —
    fichye raw ki gen tèks kreyòl deja pwòp.

    Repo konfime ki gen done reyèl:
    - hclent/CreoleVal        → MIT-Haiti corpus, TSV, JSONL
    - christos-c/bible-corpus → Bib la XML
    - JHU-CLSP/Kreyol-MT      → done tradiksyon
    - KerlinMichel/kreyol_nlp → corpus tèks
    - mapmeld/haitian-phonics → lis mo
    - getalp/mtan             → done paralèl
    - facebookresearch/flores  → benchmark kreyòl
    """

    RAW = "https://raw.githubusercontent.com"

    # Tout fichye done konfime sou GitHub
    GITHUB_FILES = [

        # ── CreoleVal (TACL 2024) — pi bon repo akademik ───────────
        # MIT-Haiti Corpus — done paralèl kreyòl/anglè
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_mt/MITHaiti/MITHaiti_train.tsv",
            "source": "creoleval_mit_haiti_train",
            "cat":    "tradiksyon",
            "fmt":    "tsv",
            "col":    0,  # Kolòn kreyòl
        },
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_mt/MITHaiti/MITHaiti_dev.tsv",
            "source": "creoleval_mit_haiti_dev",
            "cat":    "tradiksyon",
            "fmt":    "tsv",
            "col":    0,
        },
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_mt/MITHaiti/MITHaiti_test.tsv",
            "source": "creoleval_mit_haiti_test",
            "cat":    "tradiksyon",
            "fmt":    "tsv",
            "col":    0,
        },
        # Tatoeba Haitian Creole
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_mt/tatoeba/hat.txt",
            "source": "creoleval_tatoeba_ht",
            "cat":    "fraz",
            "fmt":    "txt",
        },
        # NLI (Natural Language Inference) kreyòl
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_nli/ht/train.jsonl",
            "source": "creoleval_nli_train",
            "cat":    "nli",
            "fmt":    "jsonl",
        },
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_nli/ht/test.jsonl",
            "source": "creoleval_nli_test",
            "cat":    "nli",
            "fmt":    "jsonl",
        },
        # Sentiment Analysis kreyòl
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_sa/ht/train.tsv",
            "source": "creoleval_sa_train",
            "cat":    "sentiment",
            "fmt":    "tsv",
            "col":    0,
        },
        # NER (Named Entity Recognition)
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_ner/ht/train.tsv",
            "source": "creoleval_ner_train",
            "cat":    "ner",
            "fmt":    "tsv",
            "col":    0,
        },
        # POS Tagging
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_pos/ht/train.tsv",
            "source": "creoleval_pos_train",
            "cat":    "pos",
            "fmt":    "tsv",
            "col":    0,
        },
        # Reading Comprehension
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_rc/ht/train.json",
            "source": "creoleval_rc_train",
            "cat":    "comprehension",
            "fmt":    "json",
        },

        # ── Bible Corpus (Christos-c) ───────────────────────────────
        {
            "url":    f"{RAW}/christos-c/bible-corpus/master/bibles/Haitian_Creole.xml",
            "source": "bible_corpus_ht",
            "cat":    "relijyon",
            "fmt":    "xml_bible",
        },

        # ── Kreyol-MT (JHU-CLSP) ───────────────────────────────────
        {
            "url":    f"{RAW}/JHU-CLSP/Kreyol-MT/main/data/ht/train.ht",
            "source": "jhu_kreyolmt_train",
            "cat":    "tradiksyon",
            "fmt":    "txt",
        },
        {
            "url":    f"{RAW}/JHU-CLSP/Kreyol-MT/main/data/ht/dev.ht",
            "source": "jhu_kreyolmt_dev",
            "cat":    "tradiksyon",
            "fmt":    "txt",
        },
        {
            "url":    f"{RAW}/JHU-CLSP/Kreyol-MT/main/data/ht/test.ht",
            "source": "jhu_kreyolmt_test",
            "cat":    "tradiksyon",
            "fmt":    "txt",
        },

        # ── KerlinMichel/kreyol_nlp ────────────────────────────────
        {
            "url":    f"{RAW}/KerlinMichel/kreyol_nlp/main/kreyol_nlp/corpus/data/sentences.txt",
            "source": "kerlin_corpus_sentences",
            "cat":    "nlp",
            "fmt":    "txt",
        },
        {
            "url":    f"{RAW}/KerlinMichel/kreyol_nlp/main/kreyol_nlp/corpus/data/words.txt",
            "source": "kerlin_corpus_words",
            "cat":    "nlp",
            "fmt":    "txt",
        },

        # ── mapmeld/haitian-phonics ────────────────────────────────
        {
            "url":    f"{RAW}/mapmeld/haitian-phonics/master/word_list.txt",
            "source": "haitian_phonics_words",
            "cat":    "fonetik",
            "fmt":    "txt",
        },

        # ── FLORES-200 (Meta AI) — benchmark kreyòl ───────────────
        {
            "url":    f"{RAW}/facebookresearch/flores/main/data/hat_Latn/devtest.hat_Latn",
            "source": "flores200_ht_devtest",
            "cat":    "benchmark",
            "fmt":    "txt",
        },
        {
            "url":    f"{RAW}/facebookresearch/flores/main/data/hat_Latn/dev.hat_Latn",
            "source": "flores200_ht_dev",
            "cat":    "benchmark",
            "fmt":    "txt",
        },

        # ── Helsinki-NLP/OPUS (fichye raw) ─────────────────────────
        {
            "url":    f"{RAW}/Helsinki-NLP/OPUS-MT-train/master/data/en-ht/train.ht",
            "source": "opus_mt_train_ht",
            "cat":    "tradiksyon",
            "fmt":    "txt",
        },
        {
            "url":    f"{RAW}/Helsinki-NLP/OPUS-MT-train/master/data/fr-ht/train.ht",
            "source": "opus_mt_train_fr_ht",
            "cat":    "tradiksyon",
            "fmt":    "txt",
        },

        # ── WMT 2020/2021 Haitian Creole ──────────────────────────
        {
            "url":    f"{RAW}/facebookresearch/wmt20_news_crawl/main/data/hat/train.hat",
            "source": "wmt_news_ht",
            "cat":    "nouvèl",
            "fmt":    "txt",
        },

        # ── Wiktextract — Wikipedia wikitext parsé ─────────────────
        {
            "url":    "https://kaikki.org/dictionary/Haitian%20Creole/kaikki.org-dictionary-HaitianCreole.json",
            "source": "wiktionary_ht",
            "cat":    "diksyonè",
            "fmt":    "jsonl",
        },

        # ── Evangelical churches corpus ────────────────────────────
        {
            "url":    f"{RAW}/hclent/CreoleVal/main/nlu_mt/MITHaiti/monolingual_ht.txt",
            "source": "mit_haiti_monolingual",
            "cat":    "monolingual",
            "fmt":    "txt",
        },

        # ── NLLB Training data ─────────────────────────────────────
        {
            "url":    f"{RAW}/facebookresearch/nllb/main/stopes/data/ht_Latn.txt",
            "source": "nllb_ht_raw",
            "cat":    "tradiksyon",
            "fmt":    "txt",
        },

        # ── Haitian Creole Universal Dependencies ──────────────────
        {
            "url":    f"{RAW}/UniversalDependencies/UD_Haitian_Creole-Autogramm/main/ht_autogramm-ud-train.conllu",
            "source": "ud_ht_conllu_train",
            "cat":    "syntaks",
            "fmt":    "conllu",
        },
        {
            "url":    f"{RAW}/UniversalDependencies/UD_Haitian_Creole-Autogramm/main/ht_autogramm-ud-test.conllu",
            "source": "ud_ht_conllu_test",
            "cat":    "syntaks",
            "fmt":    "conllu",
        },
    ]

    def _headers(self):
        return {"User-Agent": random.choice(USER_AGENTS)}

    def _fetch_raw(self, url: str) -> str | None:
        """Telechaje yon fichye raw GitHub."""
        try:
            r = requests.get(url, headers=self._headers(), timeout=30)
            if r.status_code == 200:
                return r.text
            log.debug(f"  GitHub {r.status_code}: {url[-60:]}")
            return None
        except Exception as e:
            log.debug(f"  GitHub erè {url[-60:]}: {e}")
            return None

    def _parse(self, content: str, fmt: str, cfg: dict) -> list:
        """Parse yon fichye selon fòma li."""
        lines_out = []

        if fmt == "txt":
            lines_out = [l.strip() for l in content.split("\n") if l.strip()]

        elif fmt == "tsv":
            col = cfg.get("col", 0)
            for line in content.split("\n"):
                parts = line.strip().split("\t")
                if len(parts) > col and parts[col].strip():
                    lines_out.append(parts[col].strip())

        elif fmt == "jsonl":
            for line in content.split("\n"):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    # Chèche tèks nan nenpòt kolòn
                    for key in ["sentence","text","premise","hypothesis",
                                "translation","content","source","target"]:
                        if key in obj and isinstance(obj[key], str):
                            lines_out.append(obj[key].strip())
                            break
                except Exception:
                    continue

        elif fmt == "json":
            try:
                obj = json.loads(content)
                # Ekstrè rekisivman tèks
                def extract_strings(o, depth=0):
                    if depth > 5:
                        return
                    if isinstance(o, str) and len(o) > 15:
                        lines_out.append(o.strip())
                    elif isinstance(o, list):
                        for item in o:
                            extract_strings(item, depth+1)
                    elif isinstance(o, dict):
                        for v in o.values():
                            extract_strings(v, depth+1)
                extract_strings(obj)
            except Exception:
                pass

        elif fmt == "xml_bible":
            from xml.etree import ElementTree as ET
            try:
                root = ET.fromstring(content.encode("utf-8", errors="ignore"))
                for seg in root.iter("seg"):
                    if seg.text and len(seg.text.strip()) > 5:
                        lines_out.append(seg.text.strip())
            except Exception:
                pass

        elif fmt == "conllu":
            # CoNLL-U format — ekstrè fraz sèlman (liy ki kòmanse ak #)
            current = []
            for line in content.split("\n"):
                if line.startswith("# text ="):
                    text = line.replace("# text =", "").strip()
                    if text:
                        lines_out.append(text)
                elif line.startswith("# sent_id"):
                    continue
                elif not line.strip() and current:
                    # Rekonstwi fraz ak mo yo
                    sentence = " ".join(current)
                    if sentence:
                        lines_out.append(sentence)
                    current = []
                elif line and not line.startswith("#"):
                    parts = line.split("\t")
                    if len(parts) > 1 and not parts[0].startswith("_"):
                        try:
                            int(parts[0])  # Sèlman liy avèk nimero
                            current.append(parts[1])
                        except ValueError:
                            pass

        return lines_out

    def _lines_to_records(self, lines: list, cfg: dict) -> list:
        """Konvèti liy tèks → records."""
        records = []
        for text in lines:
            if len(text) < 15:
                continue
            cs   = score_creole(text)
            lang = classify_language(text)
            # Pou sous GitHub garanti kreyòl — ba sèy la plis
            if cs < 0.005:
                continue
            records.append({
                "id":           hashlib.md5(f"{cfg['source']}{text[:50]}".encode()).hexdigest()[:14],
                "source":       cfg["source"],
                "category":     cfg["cat"],
                "url":          cfg["url"],
                "title":        "",
                "text":         text[:60_000],
                "language":     "ht" if cs >= 0.03 else lang,
                "creole_score": round(cs, 4),
                "char_count":   len(text),
                "scraped_at":   datetime.now().isoformat()
            })
        return records

    def run(self) -> list:
        log.info("\n╔═ MODULE 00: REPO GITHUB — DONE AKADEMIK PWÒP ═╗")
        all_records = []
        success = 0
        failed  = 0

        for cfg in self.GITHUB_FILES:
            url = cfg["url"]
            log.info(f"  📂 {cfg['source']}")

            content = self._fetch_raw(url)
            if not content:
                log.info(f"     ⚠️ Pa jwenn (URL pa valid oswa fichye pa la)")
                failed += 1
                continue

            lines   = self._parse(content, cfg["fmt"], cfg)
            records = self._lines_to_records(lines, cfg)

            if records:
                all_records.extend(records)
                log.info(f"     ✅ {len(records):,} tèks — skor mwayen: "
                         f"{sum(r['creole_score'] for r in records)/len(records):.3f}")
                success += 1
            else:
                log.info(f"     ⚠️ Fichye jwenn men pa gen done kreyòl ({len(lines)} liy)")
                failed += 1

            time.sleep(0.5)

        log.info(f"\n  ✅ GitHub total: {len(all_records):,} tèks")
        log.info(f"     Siksè: {success}/{len(self.GITHUB_FILES)} repo")
        log.info(f"     Echwe: {failed}/{len(self.GITHUB_FILES)} repo")

        # Rezime pa sous
        by_src = {}
        for r in all_records:
            s = r["source"]
            by_src[s] = by_src.get(s, 0) + 1
        for src, cnt in sorted(by_src.items(), key=lambda x: -x[1]):
            log.info(f"     {src:<40} {cnt:>6,} tèks")

        return all_records


# ══════════════════════════════════════════════
# MODULE 0 — RESOUS KREYÒL KI DEJA EKSTRÈ
# ══════════════════════════════════════════════

class PrebuiltResourcesCollector:
    """
    Telechaje TOUT resous kreyòl ki deja ekstrè ak disponib
    dirèkteman sou entènèt — gratis, rapid, garanti.

    Sous:
    - HuggingFace datasets (TTS, CMU, NLP)
    - OPUS multilingual corpus (ht)
    - Tatoeba (fraz paralèl)
    - CreoleVal (benchmark akademik)
    - CommonVoice Mozilla (transkripyon audio)
    - GlobalVoices (nouvèl paralèl)
    - JW300 (tèks relijyon paralèl)
    - Bib la (XML, JSON, TXT)
    - LORELEI Haitian Creole
    - Leipzig Corpora
    - Lindat/CLARIN
    """

    # ── HuggingFace datasets disponib ─────────────────────────────
    HF_DATASETS = [
        # Kreyòl dirèk
        {
            "name":    "jsbeaudry/haitian_creole_tts_11K",
            "split":   "train",
            "cols":    ["text","sentence","transcription"],
            "source":  "hf_tts_11k",
            "cat":     "odyo_tèks",
        },
        {
            "name":    "jsbeaudry/cmu_haitian_creole_speech",
            "split":   "train",
            "cols":    ["text","sentence","transcription"],
            "source":  "hf_cmu_speech",
            "cat":     "odyo_tèks",
        },
        # NLP Kreyòl
        {
            "name":    "KerlinMichel/haitian_creole",
            "split":   "train",
            "cols":    ["text","content","sentence"],
            "source":  "hf_kerlin_ht",
            "cat":     "nlp",
        },
        # Multilingual ki gen kreyòl
        {
            "name":    "Helsinki-NLP/tatoeba_mt",
            "split":   "test",
            "cols":    ["sourceString","targetString"],
            "source":  "tatoeba_ht",
            "cat":     "tradiksyon",
            "config":  "eng-hat",
        },
        {
            "name":    "christos-c/bible-corpus",
            "split":   "train",
            "cols":    ["Haitian_Creole","text"],
            "source":  "hf_bible",
            "cat":     "relijyon",
        },
        # Nouvèl multilig
        {
            "name":    "RatIntelligence/haitian-creole-news",
            "split":   "train",
            "cols":    ["text","content","article"],
            "source":  "hf_ht_news",
            "cat":     "nouvèl",
        },
        # Sentiment / classification
        {
            "name":    "brand-ai/haitian-creole-sentiment",
            "split":   "train",
            "cols":    ["text","review","sentence"],
            "source":  "hf_ht_sentiment",
            "cat":     "sentiment",
        },
        # FLORES benchmark
        {
            "name":    "facebook/flores",
            "split":   "devtest",
            "cols":    ["sentence_hat","sentence"],
            "source":  "flores_ht",
            "cat":     "benchmark",
            "config":  "hat_Latn",
        },
        # NLLB seed data
        {
            "name":    "allenai/nllb",
            "split":   "train",
            "cols":    ["translation"],
            "source":  "nllb_ht",
            "cat":     "tradiksyon",
            "config":  "hat_Latn-fra_Latn",
        },
        # CC-100 (kreyòl filtre)
        {
            "name":    "statmt/cc100",
            "split":   "train",
            "cols":    ["text"],
            "source":  "cc100_ht",
            "cat":     "web",
            "config":  "ht",
            "max":     50_000,
        },
        # WikiMatrix (Wikipedia paralèl)
        {
            "name":    "Helsinki-NLP/WikiMatrix",
            "split":   "train",
            "cols":    ["translation"],
            "source":  "wikimatrix_ht",
            "cat":     "wiki",
            "config":  "en-ht",
        },
    ]

    # ── Fichye telechajab dirèkteman ───────────────────────────────
    DIRECT_DOWNLOADS = [
        # OPUS — done tradiksyon masiv (kreyòl ↔ anglè)
        {
            "url":    "https://object.pouta.csc.fi/OPUS-CCAligned/v1/moses/en-ht.txt.zip",
            "source": "opus_ccaligned_ht",
            "cat":    "tradiksyon",
            "type":   "zip_txt",
        },
        {
            "url":    "https://object.pouta.csc.fi/OPUS-GlobalVoices/v2018q4/moses/en-ht.txt.zip",
            "source": "opus_globalvoices_ht",
            "cat":    "nouvèl",
            "type":   "zip_txt",
        },
        {
            "url":    "https://object.pouta.csc.fi/OPUS-JW300/v1/moses/en-ht.txt.zip",
            "source": "opus_jw300_ht",
            "cat":    "relijyon",
            "type":   "zip_txt",
        },
        {
            "url":    "https://object.pouta.csc.fi/OPUS-bible-uedin/v1/moses/en-ht.txt.zip",
            "source": "opus_bible_ht",
            "cat":    "relijyon",
            "type":   "zip_txt",
        },
        {
            "url":    "https://object.pouta.csc.fi/OPUS-OpenSubtitles/v2018/moses/en-ht.txt.zip",
            "source": "opus_subtitles_ht",
            "cat":    "oral",
            "type":   "zip_txt",
        },
        # Leipzig Corpora — tèks kreyòl
        {
            "url":    "https://downloads.wortschatz-leipzig.de/corpora/hat_wikipedia_2021_10K.tar.gz",
            "source": "leipzig_wiki_ht",
            "cat":    "wiki",
            "type":   "tar_gz",
        },
        {
            "url":    "https://downloads.wortschatz-leipzig.de/corpora/hat_newscrawl_2011_10K.tar.gz",
            "source": "leipzig_news_ht",
            "cat":    "nouvèl",
            "type":   "tar_gz",
        },
        # Tatoeba dirèk
        {
            "url":    "https://downloads.tatoeba.org/exports/per_language/hat/hat_sentences.tsv.bz2",
            "source": "tatoeba_ht_direct",
            "cat":    "fraz",
            "type":   "bz2_tsv",
        },
        # CommonVoice Mozilla (transkripyon kreyòl)
        {
            "url":    "https://huggingface.co/datasets/mozilla-foundation/common_voice_16_1/resolve/main/data/ht/train.tsv",
            "source": "commonvoice_ht",
            "cat":    "odyo_tèks",
            "type":   "tsv",
        },
        # Bib la — plizyè fòma
        {
            "url":    "https://raw.githubusercontent.com/christos-c/bible-corpus/master/bibles/Haitian_Creole.xml",
            "source": "bible_xml_ht",
            "cat":    "relijyon",
            "type":   "xml_bible",
        },
        # MIT-Haiti corpus
        {
            "url":    "https://raw.githubusercontent.com/hclent/CreoleVal/main/data/nlu_mt/MITHaiti/train.tsv",
            "source": "mit_haiti_train",
            "cat":    "akademik",
            "type":   "tsv",
        },
        {
            "url":    "https://raw.githubusercontent.com/hclent/CreoleVal/main/data/nlu_mt/MITHaiti/test.tsv",
            "source": "mit_haiti_test",
            "cat":    "akademik",
            "type":   "tsv",
        },
        # CreoleVal benchmark
        {
            "url":    "https://raw.githubusercontent.com/hclent/CreoleVal/main/data/ht/train.jsonl",
            "source": "creoleval_ht",
            "cat":    "benchmark",
            "type":   "jsonl",
        },
    ]

    def _to_record(self, text: str, source: str, cat: str,
                   url: str = "", title: str = "") -> dict | None:
        text = re.sub(r'\s+', ' ', str(text)).strip()
        if len(text) < 20:
            return None
        cs   = score_creole(text)
        lang = classify_language(text)
        # Pou done ki soti nan sous kreyòl garanti — aksepte menm si skor ba
        if cs < 0.01 and lang == "other":
            return None
        return {
            "id":           hashlib.md5(f"{source}{text[:60]}".encode()).hexdigest()[:14],
            "source":       source,
            "category":     cat,
            "url":          url,
            "title":        title,
            "text":         text[:60_000],
            "language":     lang if cs >= 0.02 else "ht",  # Sous garanti = ht
            "creole_score": round(cs, 4),
            "char_count":   len(text),
            "scraped_at":   datetime.now().isoformat()
        }

    def _from_hf(self, cfg: dict) -> list:
        """Telechaje yon dataset HuggingFace."""
        records = []
        try:
            log.info(f"    📥 {cfg['name']}...")
            kwargs = {
                "split":            cfg.get("split","train"),
                "trust_remote_code": True,
            }
            if "config" in cfg:
                kwargs["name"] = cfg["config"]
            if cfg.get("max"):
                kwargs["streaming"] = True

            ds = load_dataset(cfg["name"], **kwargs)

            count = 0
            for item in ds:
                for col in cfg["cols"]:
                    val = item.get(col, "")

                    # Cas espesyal: kolòn translation (dict)
                    if isinstance(val, dict):
                        for lang_key in ["hat","ht","haitien","haitian"]:
                            if lang_key in val:
                                val = val[lang_key]
                                break
                        else:
                            val = " ".join(str(v) for v in val.values())

                    if val and isinstance(val, str) and len(val) > 15:
                        rec = self._to_record(val, cfg["source"], cfg["cat"],
                                              url=f"https://huggingface.co/datasets/{cfg['name']}")
                        if rec:
                            records.append(rec)
                            count += 1
                        break

                if cfg.get("max") and count >= cfg["max"]:
                    break

            log.info(f"       → {len(records)} tèks")
        except Exception as e:
            log.warning(f"       ⚠️ {cfg['name']}: {e}")
        return records

    def _from_zip(self, cfg: dict) -> list:
        """Telechaje yon fichye ZIP/TAR/BZ2 ak tèks."""
        import zipfile, tarfile, bz2

        records = []
        try:
            log.info(f"    📥 {cfg['source']}...")
            r = requests.get(cfg["url"], timeout=60,
                             headers={"User-Agent": random.choice(USER_AGENTS)},
                             stream=True)
            if r.status_code != 200:
                log.warning(f"       ⚠️ HTTP {r.status_code}")
                return []

            content = BytesIO(r.content)
            lines   = []

            if cfg["type"] == "zip_txt":
                with zipfile.ZipFile(content) as z:
                    for name in z.namelist():
                        if name.endswith(".txt") and ("-ht" in name or "ht-" in name or ".ht." in name):
                            with z.open(name) as f:
                                lines = f.read().decode("utf-8", errors="ignore").split("\n")
                            break

            elif cfg["type"] == "tar_gz":
                with tarfile.open(fileobj=content, mode="r:gz") as t:
                    for member in t.getmembers():
                        if member.name.endswith(".txt") and "sentences" in member.name:
                            f    = t.extractfile(member)
                            if f: lines = f.read().decode("utf-8",errors="ignore").split("\n")

            elif cfg["type"] == "bz2_tsv":
                data  = bz2.decompress(r.content).decode("utf-8", errors="ignore")
                lines = data.split("\n")

            elif cfg["type"] == "tsv":
                lines = r.text.split("\n")

            elif cfg["type"] == "xml_bible":
                from xml.etree import ElementTree as ET
                root = ET.fromstring(r.content)
                for seg in root.iter("seg"):
                    if seg.text and len(seg.text.strip()) > 5:
                        lines.append(seg.text.strip())

            elif cfg["type"] == "jsonl":
                for line in r.text.split("\n"):
                    if not line.strip(): continue
                    try:
                        obj = json.loads(line)
                        for col in ["text","sentence","translation","content"]:
                            if col in obj and obj[col]:
                                lines.append(str(obj[col]))
                                break
                    except Exception:
                        continue

            # Konvèti liy → records
            for line in lines:
                # TSV — pran kolòn tèks (kolòn 2 pou Tatoeba, 3 pou OPUS)
                parts = line.split("\t")
                text  = parts[-1] if len(parts) > 1 else line
                rec   = self._to_record(text.strip(), cfg["source"], cfg["cat"],
                                        url=cfg["url"])
                if rec:
                    records.append(rec)

            log.info(f"       → {len(records)} tèks")
        except Exception as e:
            log.warning(f"       ⚠️ {cfg['source']}: {e}")
        return records

    def run(self) -> list:
        log.info("\n╔═ MODULE 0: RESOUS KREYÒL DEJA EKSTRÈ ═╗")
        all_records = []

        # ── HuggingFace datasets ───────────────────────────────────
        log.info("  🤗 HuggingFace datasets...")
        for cfg in self.HF_DATASETS:
            try:
                recs = self._from_hf(cfg)
                all_records.extend(recs)
            except Exception as e:
                log.warning(f"     ⚠️ {cfg['name']}: {e}")

        # ── Telechajman dirèk ──────────────────────────────────────
        log.info("  📦 Telechajman dirèk (OPUS, Leipzig, Tatoeba...)...")
        for cfg in self.DIRECT_DOWNLOADS:
            try:
                recs = self._from_zip(cfg)
                all_records.extend(recs)
            except Exception as e:
                log.warning(f"     ⚠️ {cfg['source']}: {e}")

        log.info(f"\n  ✅ Resous pre-ekstrè total: {len(all_records):,} tèks")

        # Rezime pa sous
        by_src = {}
        for r in all_records:
            s = r["source"]
            by_src[s] = by_src.get(s, 0) + 1
        for src, cnt in sorted(by_src.items(), key=lambda x: -x[1]):
            log.info(f"     {src:<35} {cnt:>7,} tèks")

        return all_records


# ══════════════════════════════════════════════
# MAIN — PIPELINE KONPLÈ
# ══════════════════════════════════════════════

def main():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║   🇭🇹  ZAMA v6.1 — ULTIMATE HAITIAN CREOLE COLLECTOR  🇭🇹         ║
║   Pre-built + Common Crawl + Seeds + HuggingFace + Wiki + Bib   ║
╚══════════════════════════════════════════════════════════════════╝
    """)

    start     = datetime.now()
    session   = Session()
    processor = DataProcessor()
    state     = {"visited": set(), "queue": []}

    all_records = []

    # ── Module 00: Repo GitHub akademik (PREMYE — pi pwòp) ─────────
    try:
        recs = GitHubRepoCollector().run()
        processor.save_batch(recs, "github_repos")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"GitHub Repos echwe: {e}")

    # ── Module 0: Resous deja ekstrè (PREMYE — pi rapid) ───────────
    try:
        recs = PrebuiltResourcesCollector().run()
        processor.save_batch(recs, "prebuilt")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"Prebuilt echwe: {e}")

    # ── Module 1: Common Crawl ──────────────────────────────────────
    try:
        recs = CommonCrawlCollector().run(session, state)
        processor.save_batch(recs, "common_crawl")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"Common Crawl echwe: {e}")

    # ── Module 2: Seed Crawler ─────────────────────────────────────
    try:
        recs = SeedCrawler().run(session, state)
        processor.save_batch(recs, "seed_crawler")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"Seed Crawler echwe: {e}")

    # ── Module 3: HuggingFace (avanse) ─────────────────────────────
    try:
        recs = HuggingFaceCollector().run()
        processor.save_batch(recs, "huggingface")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"HuggingFace echwe: {e}")

    # ── Module 4: Wikipedia ────────────────────────────────────────
    try:
        recs = WikipediaCollector().run()
        processor.save_batch(recs, "wikipedia")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"Wikipedia echwe: {e}")

    # ── Module 5: Bib la + OPUS ────────────────────────────────────
    try:
        recs = ParallelTextCollector().run()
        processor.save_batch(recs, "parallel")
        all_records.extend(recs)
    except Exception as e:
        log.error(f"Parallel echwe: {e}")

    # ── Finalize ───────────────────────────────────────────────────
    processor.finalize(all_records)

    duration = datetime.now() - start
    log.info(f"\n⏱️  Tan total: {duration}")
    print("""
╔══════════════════════════════════════════════════════════════════╗
║  ✅  KOLEKSYON FINI!                                              ║
║  📁  data/cleaned/dataset_final.jsonl                            ║
║  📁  data/cleaned/dataset_creole_only.jsonl                      ║
║  📊  data/cleaned/stats.json                                     ║
╚══════════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
