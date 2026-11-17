# Kanso Offline Dictionary Data (`dictionary-app-data`)

Automated builder and GitHub Releases repository for **Kanso: Minimalist Dictionary** offline language packs (`.kdict.gz`) covering the **50 most popular Wiktionary languages**.

Each language pack is compiled into Kanso's indexed random-access binary dictionary format (`kdict-v1`)—trimmed strictly to the fields Kanso renders (**headword, part of speech, IPA pronunciation, definitions + concise usage examples, etymologies, synonyms, and antonyms**)—so after streaming gunzip decompression on-device, lookups run in `< 1 ms` with zero resident memory overhead.

---

## Updating Dictionary Releases Anytime

Make sure Python 3.10+ and the GitHub CLI (`gh auth status`) are available. You can rerun [`scripts/update_dictionaries.py`](./scripts/update_dictionaries.py) at any time to refresh **one**, **several**, or **all 50** language `.gz` releases:

```bash
# View all 50 supported languages and their current sizes / release timestamps
python3 scripts/update_dictionaries.py --list

# Update a single language (e.g. English)
python3 scripts/update_dictionaries.py --langs en

# Update multiple languages of your choice
python3 scripts/update_dictionaries.py --langs es fr de ja zh

# Build & publish only languages not yet in manifest.json
python3 scripts/update_dictionaries.py --missing-only

# Rebuild & update ALL 50 language releases
python3 scripts/update_dictionaries.py --all
```

### Local-Only Build & Inspection (No Upload)
```bash
python3 scripts/update_dictionaries.py --langs en --no-upload --keep-local
python3 scripts/inspect_kdict.py dist/en.kdict serendipity dictionary went
```

---

## Release & URL Structure

- **Master Manifest Release (`manifest`):**
  `https://github.com/JesseCan9000/dictionary-app-data/releases/download/manifest/manifest.json`
  *(Also committed at [`manifest.json`](./manifest.json) in this repository.)*
- **Per-Language Releases (`lang-<code>`):**
  `https://github.com/JesseCan9000/dictionary-app-data/releases/download/lang-<code>/<code>.kdict.gz`
  *(e.g. `lang-en/en.kdict.gz`, `lang-es/es.kdict.gz`, `lang-ja/ja.kdict.gz`, ...)*

---

## Data License & Attribution

- **Build Scripts:** MIT License — Copyright © 2026 Canavan Industries LLC.
- **Dictionary Data:** Extracted from [Wiktionary](https://www.wiktionary.org) via [Kaikki.org / Wiktextract](https://kaikki.org) (by Tatu Ylönen) and redistributed under the [Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0)](https://creativecommons.org/licenses/by-sa/4.0/). Entries are filtered and indexed into `.kdict` binary format for compact offline lookup.
