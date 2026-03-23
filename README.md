# 🇭🇹 ZAMA v3.0 — Premye Dataset Kreyòl Ayisyen pou LLM

> *Zama* vle di **"konesans"** an kreyòl.

Pwojè sa a kolekte done tèks kreyòl ayisyen sou 40+ sous pou antrene premye gwo modèl lang (LLM) nasyonal Ayiti.

---

## 📦 Sous Done (40+)

### Medya & Nouvèl
| Sit | Lang | Estati |
|-----|------|--------|
| ht.wikipedia.org | ht | ✅ |
| alterpresse.org (AlterKreyòl) | ht/fr | ✅ |
| rfi.fr/ht | ht | ✅ |
| voanouvel.com | ht | ✅ |
| lenouvelliste.com | fr/ht | ✅ |
| rezonodwes.com | ht | ✅ |
| haitilibre.com | fr | ✅ |
| vantbefinfo.com | ht | ✅ |
| balistrad.com | ht | ✅ |
| tripfoumi.com | ht | ✅ |
| haitiliberte.com | ht/fr | ✅ |
| lenational.org | fr | ✅ |
| loophaiti.com | fr/ht | ✅ |
| radiotelevisioncaraibes.com | fr/ht | ✅ |
| radyoteman.com | ht | ✅ |
| boukanews.com | ht | ✅ |

### Literati & Kilti
| Sit | Kontni |
|-----|--------|
| potomitan.info | Literati kreyòl |
| espaskreyol.org | Espace kreyòl |
| tanbou.com | Mizik, kilti |
| woymagazine.com | Magazin kreyòl |

### Relijyon (Done Paralèl Masiv)
| Sit | Kontni |
|-----|--------|
| jw.org/ht | Done paralèl ht/fr/en |
| Bible Corpus | 31k+ vèsè kreyòl |

### Done Pre-kolekte (HuggingFace)
| Dataset | Kantite |
|---------|---------|
| CMU Haitian Creole Speech | 11k+ pè |
| Haitian Creole TTS | 15h audio |

---

## 🧠 Fonksyon Entèlijan

- **SmartSession**: Rotation User-Agent, retry otomatik, backoff eksponetyèl
- **Anti-404**: Deteksyon epi skip otomatik pou URL ki pa egziste
- **Anti-ban**: Blacklist domèn ki bloke, respekte rate limits
- **Smart extraction**: 3 estrateji pou jwenn tèks prensipal la
- **Deteksyon lang**: Identifye ht/fr/mix san libreri ekstèn
- **Déduplikasyon**: Hash-based dedup sou ID ak kontni

---

## 🚀 Kijan pou Kouri

### Lokal
```bash
pip install -r requirements.txt
python scripts/scraper.py
```

### GitHub Actions (Rekòmande — dòmi pandan l kouri)
1. Fork repo sa a sou GitHub
2. **Actions** → **Zama — Haitian Creole Data Collection**
3. **Run workflow** → chwazi `full`
4. Dòmi 😴 — done ap tann ou nan **Artifacts** maten an

---

## 📊 Rezilta Atann

```
Wikipedia        ~80MB
Medya ayisyen   ~150MB
Literati         ~30MB
Relijyon        ~50MB
HuggingFace     ~100MB
─────────────────────
TOTAL           ~400MB
```

---

## 📄 Lisans

MIT License — Lib pou tout moun itilize, modifye, ak pataje.

---

*Fèt ak ❤️ pou Ayiti 🇭🇹*
