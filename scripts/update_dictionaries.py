#!/usr/bin/env python3
"""
Kanso Offline Dictionary Builder & GitHub Releases Publisher
============================================================
Streams pre-parsed Wiktionary data (Kaikki.org / Wiktextract, CC BY-SA 4.0),
trims entries strictly to Kanso's display fields (headword, POS, IPA,
definitions + concise usage examples, etymologies, synonyms, antonyms),
compiles indexed O(1) random-access `.kdict` v1 files, compresses them to
`.kdict.gz`, verifies binary integrity, and publishes per-language GitHub
Releases (`lang-<code>`) plus the master `manifest` Release (`manifest.json`).

Usage examples:
  # List all 50 supported languages & current release status
  python3 scripts/update_dictionaries.py --list

  # Build & publish English only
  python3 scripts/update_dictionaries.py --langs en

  # Build & publish specific languages at your choosing
  python3 scripts/update_dictionaries.py --langs es fr de ja

  # Build & publish any of the 50 languages not yet in manifest.json
  python3 scripts/update_dictionaries.py --missing-only

  # Rebuild & republish ALL 50 languages
  python3 scripts/update_dictionaries.py --all
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import gzip
import hashlib
import html
import json
import mmap
import os
import re
import shutil
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import zlib
from pathlib import Path
from typing import Any, Iterator

ROOT_DIR = Path(__file__).resolve().parent.parent
LANGUAGES_FILE = ROOT_DIR / "languages.json"
MANIFEST_FILE = ROOT_DIR / "manifest.json"
WORK_DIR = ROOT_DIR / ".work"
DIST_DIR = ROOT_DIR / "dist"

DEFAULT_GITHUB_REPO = "JesseCan9000/dictionary-app-data"
USER_AGENT = "KansoDictionaryBuilder/1.0 (+https://canavanindustries.org)"

KDICT_MAGIC = b"KDIC"
KDICT_END_MAGIC = b"KEND"
KDICT_VERSION = 1

HEADER_STRUCT = struct.Struct("<4sIIIQQQQ16s")  # 64 bytes
BUCKET_STRUCT = struct.Struct("<II")           # 8 bytes
SLOT_STRUCT = struct.Struct("<IIHHI")          # 16 bytes
FOOTER_STRUCT = struct.Struct("<IQ4s")         # 16 bytes

POS_DISPLAY_MAP: dict[str, str] = {
    "noun": "Noun",
    "verb": "Verb",
    "adj": "Adjective",
    "adv": "Adverb",
    "name": "Proper noun",
    "pron": "Pronoun",
    "prep": "Preposition",
    "postp": "Postposition",
    "conj": "Conjunction",
    "intj": "Interjection",
    "det": "Determiner",
    "article": "Article",
    "num": "Numeral",
    "particle": "Particle",
    "phrase": "Phrase",
    "prep_phrase": "Prepositional phrase",
    "proverb": "Proverb",
    "prefix": "Prefix",
    "suffix": "Suffix",
    "infix": "Infix",
    "interfix": "Interfix",
    "circumfix": "Circumfix",
    "affix": "Affix",
    "abbrev": "Abbreviation",
    "contraction": "Contraction",
    "symbol": "Symbol",
    "character": "Character",
    "romanization": "Romanization",
    "classifier": "Classifier",
    "counter": "Counter",
}

MAX_SENSES_PER_POS = 20
MAX_GLOSS_CHARS = 550
MAX_EXAMPLE_CHARS = 180
MAX_ETYM_CHARS = 900
MAX_NYMS_PER_POS = 18


def fnv1a_32(data: bytes) -> int:
    h = 0x811C9DC5
    for b in data:
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def next_pow2(n: int) -> int:
    if n <= 1024:
        return 1024
    return 1 << (n - 1).bit_length()


def clean_etymology(raw_etym: str | None) -> str | None:
    if not raw_etym:
        return None
    text = raw_etym.strip()
    if not text:
        return None

    # Strip Wiktionary {{etymon}} vertical tree preamble if present
    if text.startswith("Etymology tree\n"):
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        prose_candidates = [
            ln
            for ln in lines[1:]
            if len(ln) > 45
            or ". " in ln
            or ln.startswith(
                (
                    "From ",
                    "Borrowed ",
                    "Inherited ",
                    "Learned ",
                    "Compound ",
                    "Calque ",
                    "Uncertain",
                    "Unknown",
                    "Of ",
                    "A ",
                    "An ",
                    "Cognate ",
                    "Compare ",
                    "Back-formation",
                    "Blend ",
                    "Clipping ",
                    "Derived ",
                    "Coined ",
                    "Named ",
                    "Ultimately ",
                    "Via ",
                )
            )
        ]
        if prose_candidates:
            text = " ".join(prose_candidates)
        else:
            return None

    text = re.sub(r"\s+", " ", text).strip()
    if len(text) < 4:
        return None
    if len(text) > MAX_ETYM_CHARS:
        clipped = text[:MAX_ETYM_CHARS].rsplit(" ", 1)[0].rstrip(",;:")
        text = f"{clipped}…"
    return text


def format_gloss_with_form_links(
    gloss: str, sense: dict[str, Any], headword_lower: str
) -> str:
    escaped = html.escape(gloss, quote=False)
    targets: list[str] = []
    for key in ("form_of", "alt_of"):
        items = sense.get(key)
        if isinstance(items, list):
            for it in items[:2]:
                if isinstance(it, dict):
                    w = (it.get("word") or "").strip()
                    if w and w.lower() != headword_lower and len(w) <= 64:
                        targets.append(w)

    for target in targets:
        esc_target = html.escape(target, quote=False)
        href = f"/wiki/{target.replace(' ', '_')}"
        pattern = re.compile(r"(?<![\w/])" + re.escape(esc_target) + r"(?![\w<])")
        escaped, count = pattern.subn(
            f'<a href="{html.escape(href, quote=True)}">{esc_target}</a>',
            escaped,
            count=1,
        )
        if count:
            break
    return escaped


def extract_trimmed_pos_entry(raw_obj: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    word = (raw_obj.get("word") or "").strip()
    if not word or len(word) > 120:
        return None

    norm_key = word.lower()
    if len(norm_key.encode("utf-8")) > 240:
        return None

    raw_pos = (raw_obj.get("pos") or "entry").strip().lower()
    pos_title = POS_DISPLAY_MAP.get(raw_pos) or raw_pos.replace("_", " ").capitalize()

    # IPA
    ipa: str | None = None
    sounds = raw_obj.get("sounds")
    if isinstance(sounds, list):
        for s in sounds:
            if isinstance(s, dict):
                cand = (s.get("ipa") or "").strip()
                if cand and len(cand) <= 80:
                    ipa = cand
                    break

    # Etymology
    etym = clean_etymology(raw_obj.get("etymology_text"))

    # Definitions + concise example + sense-level nyms
    senses_out: list[str | list[str]] = []
    seen_glosses: set[str] = set()
    syn_list: list[str] = []
    ant_list: list[str] = []
    seen_syn: set[str] = {norm_key}
    seen_ant: set[str] = {norm_key}

    def add_nym_items(items: Any, target_list: list[str], seen_set: set[str]) -> None:
        if not isinstance(items, list) or len(target_list) >= MAX_NYMS_PER_POS:
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            w = (item.get("word") or "").strip()
            if not w or len(w) > 64 or "Thesaurus:" in w:
                continue
            wl = w.lower()
            if wl in seen_set:
                continue
            seen_set.add(wl)
            target_list.append(w)
            if len(target_list) >= MAX_NYMS_PER_POS:
                break

    add_nym_items(raw_obj.get("synonyms"), syn_list, seen_syn)
    add_nym_items(raw_obj.get("antonyms"), ant_list, seen_ant)

    raw_senses = raw_obj.get("senses")
    if isinstance(raw_senses, list):
        for sense in raw_senses:
            if not isinstance(sense, dict):
                continue
            add_nym_items(sense.get("synonyms"), syn_list, seen_syn)
            add_nym_items(sense.get("antonyms"), ant_list, seen_ant)

            if len(senses_out) >= MAX_SENSES_PER_POS:
                continue

            glosses = sense.get("glosses")
            if not isinstance(glosses, list) or not glosses:
                continue
            raw_gloss = (glosses[-1] or "").strip()
            if not raw_gloss:
                continue
            raw_gloss = re.sub(r"\s+", " ", raw_gloss)
            if len(raw_gloss) > MAX_GLOSS_CHARS:
                raw_gloss = raw_gloss[:MAX_GLOSS_CHARS].rsplit(" ", 1)[0] + "…"

            gloss_dedup_key = raw_gloss.lower()
            if gloss_dedup_key in seen_glosses:
                continue
            seen_glosses.add(gloss_dedup_key)

            formatted_gloss = format_gloss_with_form_links(raw_gloss, sense, norm_key)

            # Pick 1 concise usage example (preferring type == 'example' over long book quotes)
            chosen_example: str | None = None
            examples = sense.get("examples")
            if isinstance(examples, list):
                for ex in examples:
                    if not isinstance(ex, dict):
                        continue
                    if ex.get("type") == "quotation":
                        continue
                    ex_text = re.sub(r"\s+", " ", (ex.get("text") or "").strip())
                    if 3 <= len(ex_text) <= MAX_EXAMPLE_CHARS:
                        chosen_example = ex_text
                        break

            if chosen_example:
                senses_out.append([formatted_gloss, chosen_example])
            else:
                senses_out.append(formatted_gloss)

    if not senses_out and not etym and not syn_list and not ant_list:
        return None

    record: dict[str, Any] = {
        "w": word,
        "t": pos_title,
    }
    if ipa:
        record["i"] = ipa
    if etym:
        record["e"] = etym
    if senses_out:
        record["d"] = senses_out
    if syn_list:
        record["s"] = syn_list
    if ant_list:
        record["a"] = ant_list

    return norm_key, record


def stream_jsonl_lines_with_resume(url: str, max_retries: int = 6) -> Iterator[bytes]:
    """
    Streams a remote `.jsonl.gz` (or `.jsonl`) file over HTTP without saving the raw dump to disk.
    Uses fast C `curl` + `gzip -dc` pipeline when available, falling back to Python streaming zlib.
    """
    is_gz = url.endswith(".gz")
    curl_bin = shutil.which("curl")
    gzip_bin = shutil.which("gzip")

    if is_gz and curl_bin and gzip_bin:
        for attempt in range(1, max_retries + 1):
            curl_proc = subprocess.Popen(
                [
                    curl_bin,
                    "-fsSL",
                    "--retry",
                    "4",
                    "--retry-delay",
                    "2",
                    "-A",
                    USER_AGENT,
                    url,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            gz_proc = subprocess.Popen(
                [gzip_bin, "-dc"],
                stdin=curl_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if curl_proc.stdout:
                curl_proc.stdout.close()

            try:
                assert gz_proc.stdout is not None
                for raw_line in gz_proc.stdout:
                    ln = raw_line.strip()
                    if ln:
                        yield ln
                gz_rc = gz_proc.wait()
                curl_rc = curl_proc.wait()
                if gz_rc == 0 and curl_rc == 0:
                    return
                err_msg = (
                    (gz_proc.stderr.read().decode("utf-8", "ignore") if gz_proc.stderr else "")
                    + " "
                    + (curl_proc.stderr.read().decode("utf-8", "ignore") if curl_proc.stderr else "")
                ).strip()
            finally:
                for p in (gz_proc, curl_proc):
                    if p.poll() is None:
                        p.kill()
                        p.wait()

            if attempt == max_retries:
                raise RuntimeError(
                    f"Failed streaming {url} after {max_retries} attempts: {err_msg}"
                )
            wait_s = min(15, 2**attempt)
            print(
                f"  [retry {attempt}/{max_retries}] Stream interrupted ({err_msg}); retrying in {wait_s}s...",
                flush=True,
            )
            time.sleep(wait_s)
        return

    # Pure-Python streaming fallback (supports both .jsonl.gz and uncompressed .jsonl)
    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        decomp = zlib.decompressobj(16 + zlib.MAX_WBITS) if is_gz else None
        partial = b""
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                while True:
                    chunk = resp.read(262_144)
                    if not chunk:
                        if decomp:
                            partial += decomp.flush()
                        if partial.strip():
                            for ln in partial.split(b"\n"):
                                if ln.strip():
                                    yield ln
                        return
                    raw_bytes = decomp.decompress(chunk) if decomp else chunk
                    if not raw_bytes:
                        continue
                    data = partial + raw_bytes
                    lines = data.split(b"\n")
                    partial = lines.pop()
                    for ln in lines:
                        if ln.strip():
                            yield ln
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, zlib.error) as exc:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Failed streaming {url} after {max_retries} retries: {exc}"
                ) from exc
            wait_s = min(15, 2**attempt)
            print(
                f"  [retry {attempt}/{max_retries}] Stream error ({exc}); retrying in {wait_s}s...",
                flush=True,
            )
            time.sleep(wait_s)


def merge_pos_records_for_key(pos_records: list[dict[str, Any]]) -> bytes:
    """
    Merges all single-POS records sharing a lowercase lookup key into Kanso's compact
    multi-variant JSON payload `{"v": [...]}`.
    """
    variants_by_word: dict[str, dict[str, Any]] = {}
    variant_order: list[str] = []

    for rec in pos_records:
        w = rec["w"]
        v = variants_by_word.get(w)
        if v is None:
            v = {"w": w, "i": None, "e": [], "_seen_e": set(), "p": []}
            variants_by_word[w] = v
            variant_order.append(w)

        if not v["i"] and rec.get("i"):
            v["i"] = rec["i"]

        etym = rec.get("e")
        if etym and etym not in v["_seen_e"] and len(v["e"]) < 3:
            v["_seen_e"].add(etym)
            v["e"].append(etym)

        pos_item: dict[str, Any] = {"t": rec["t"]}
        if rec.get("d"):
            pos_item["d"] = rec["d"]
        if rec.get("s"):
            pos_item["s"] = rec["s"]
        if rec.get("a"):
            pos_item["a"] = rec["a"]

        if "d" in pos_item or "s" in pos_item or "a" in pos_item:
            v["p"].append(pos_item)

    out_variants: list[dict[str, Any]] = []
    for w in variant_order:
        v = variants_by_word[w]
        if not v["p"] and not v["e"]:
            continue
        clean_v: dict[str, Any] = {"w": v["w"]}
        if v["i"]:
            clean_v["i"] = v["i"]
        if v["e"]:
            clean_v["e"] = v["e"]
        if v["p"]:
            clean_v["p"] = v["p"]
        out_variants.append(clean_v)

    payload_obj = {"v": out_variants}
    return json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def lookup_in_kdict(kdict_path: Path, query: str) -> dict[str, Any] | None:
    norm_bytes = query.strip().lower().encode("utf-8")
    if not norm_bytes:
        return None
    with kdict_path.open("rb") as f:
        header_raw = f.read(HEADER_STRUCT.size)
        if len(header_raw) != HEADER_STRUCT.size:
            raise ValueError("Truncated kdict header")
        (
            magic,
            version,
            bucket_count,
            unique_keys,
            bucket_off,
            slot_off,
            data_off,
            expected_size,
            _lang_raw,
        ) = HEADER_STRUCT.unpack(header_raw)
        if magic != KDICT_MAGIC or version != KDICT_VERSION:
            raise ValueError("Invalid kdict magic or version")

        h32 = fnv1a_32(norm_bytes)
        bucket_idx = h32 & (bucket_count - 1)
        f.seek(bucket_off + bucket_idx * BUCKET_STRUCT.size)
        slot_start, slot_count = BUCKET_STRUCT.unpack(f.read(BUCKET_STRUCT.size))
        if slot_count == 0:
            return None

        f.seek(slot_off + slot_start * SLOT_STRUCT.size)
        slots_raw = f.read(slot_count * SLOT_STRUCT.size)
        for i in range(slot_count):
            sh32, rel_lo, rel_hi, klen, plen = SLOT_STRUCT.unpack_from(
                slots_raw, i * SLOT_STRUCT.size
            )
            if sh32 != h32 or klen != len(norm_bytes):
                continue
            rel_off = rel_lo + (rel_hi << 32)
            f.seek(data_off + rel_off)
            blob = f.read(klen + plen)
            if blob[:klen] == norm_bytes:
                return json.loads(blob[klen:].decode("utf-8"))
    return None


def verify_kdict_file(kdict_path: Path, sample_keys: list[str]) -> None:
    actual_size = kdict_path.stat().st_size
    if actual_size < HEADER_STRUCT.size + FOOTER_STRUCT.size:
        raise RuntimeError(f"kdict file too small ({actual_size} bytes)")

    with kdict_path.open("rb") as f:
        (
            magic,
            version,
            bucket_count,
            unique_keys,
            _bucket_off,
            _slot_off,
            _data_off,
            expected_size,
            _lang_raw,
        ) = HEADER_STRUCT.unpack(f.read(HEADER_STRUCT.size))
        if magic != KDICT_MAGIC or version != KDICT_VERSION:
            raise RuntimeError("Header magic/version mismatch")
        if expected_size != actual_size:
            raise RuntimeError(
                f"File size mismatch: expected {expected_size}, got {actual_size}"
            )
        if unique_keys == 0 or bucket_count == 0:
            raise RuntimeError("Empty dictionary index")

        f.seek(actual_size - FOOTER_STRUCT.size)
        footer_keys, _data_len, end_magic = FOOTER_STRUCT.unpack(
            f.read(FOOTER_STRUCT.size)
        )
        if end_magic != KDICT_END_MAGIC or footer_keys != unique_keys:
            raise RuntimeError("Footer integrity check failed")

    for sk in sample_keys:
        hit = lookup_in_kdict(kdict_path, sk)
        if not hit or not hit.get("v"):
            raise RuntimeError(f"Self-test lookup failed for sample key {sk!r}")


def compress_to_gzip_and_hash(src_path: Path, gz_path: Path) -> tuple[int, str]:
    if gz_path.exists():
        gz_path.unlink()

    gzip_bin = shutil.which("gzip")
    if gzip_bin:
        with gz_path.open("wb") as out_f:
            subprocess.run(
                [gzip_bin, "-6", "-n", "-c", str(src_path)],
                stdout=out_f,
                check=True,
            )
    else:
        with src_path.open("rb") as in_f, gzip.GzipFile(
            filename="", mode="wb", fileobj=gz_path.open("wb"), compresslevel=6, mtime=0
        ) as gz_f:
            shutil.copyfileobj(in_f, gz_f, length=1024 * 1024)

    sha = hashlib.sha256()
    with gz_path.open("rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return gz_path.stat().st_size, sha.hexdigest()


def build_language_pack(lang: dict[str, Any]) -> tuple[Path, Path, dict[str, Any]]:
    code = lang["code"].lower()
    name = lang["name"]
    url = lang["kaikkiUrl"]

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    pos_temp_path = WORK_DIR / f"{code}.pos.bin"
    data_temp_path = WORK_DIR / f"{code}.data.bin"
    kdict_path = DIST_DIR / f"{code}.kdict"
    gz_path = DIST_DIR / f"{code}.kdict.gz"

    for p in (pos_temp_path, data_temp_path):
        if p.exists():
            p.unlink()

    print(f"\n=== [{code}] Building {name} from {url} ===", flush=True)
    t0 = time.time()

    # Pass 1: Stream remote JSONL -> compact POS log on disk + lightweight offset index
    key_offsets: dict[bytes, list[int]] = {}
    pos_count = 0
    raw_lines = 0
    sample_keys: list[str] = []

    try:
        with pos_temp_path.open("wb") as pos_f:
            for raw_line in stream_jsonl_lines_with_resume(url):
                raw_lines += 1
                if raw_lines % 100_000 == 0:
                    print(
                        f"  [{code}] Streamed {raw_lines:,} lines ({len(key_offsets):,} unique headwords)...",
                        flush=True,
                    )
                try:
                    raw_obj = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                extracted = extract_trimmed_pos_entry(raw_obj)
                if extracted is None:
                    continue

                norm_key, pos_rec = extracted
                rec_bytes = json.dumps(
                    pos_rec, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
                rec_len = len(rec_bytes)
                if rec_len >= (1 << 20):
                    continue

                off = pos_f.tell()
                pos_f.write(rec_bytes)
                packed = (off << 20) | rec_len

                kbytes = norm_key.encode("utf-8")
                bucket_list = key_offsets.get(kbytes)
                if bucket_list is None:
                    key_offsets[kbytes] = [packed]
                    if len(sample_keys) < 5:
                        sample_keys.append(norm_key)
                else:
                    bucket_list.append(packed)
                pos_count += 1

        unique_key_count = len(key_offsets)
        if unique_key_count == 0:
            raise RuntimeError(f"No valid entries extracted for {code} ({name})")

        bucket_count = next_pow2(unique_key_count)
        mask = bucket_count - 1
        print(
            f"  [{code}] Indexing {unique_key_count:,} unique words ({pos_count:,} POS entries) into {bucket_count:,} buckets...",
            flush=True,
        )

        # Group keys by hash bucket
        buckets: list[list[tuple[int, bytes, list[int]]]] = [
            [] for _ in range(bucket_count)
        ]
        for kbytes, packed_list in key_offsets.items():
            h32 = fnv1a_32(kbytes)
            buckets[h32 & mask].append((h32, kbytes, packed_list))
        key_offsets.clear()

        # Pass 2: Build Data Section + Bucket & Slot tables
        bucket_table = bytearray(bucket_count * BUCKET_STRUCT.size)
        slot_table = bytearray(unique_key_count * SLOT_STRUCT.size)
        slot_cursor = 0

        with pos_temp_path.open("rb") as pos_f, mmap.mmap(
            pos_f.fileno(), 0, access=mmap.ACCESS_READ
        ) as pos_mm, data_temp_path.open("wb") as data_f:
            for b_idx, items in enumerate(buckets):
                cnt = len(items)
                BUCKET_STRUCT.pack_into(
                    bucket_table, b_idx * BUCKET_STRUCT.size, slot_cursor, cnt
                )
                if cnt == 0:
                    continue

                for h32, kbytes, packed_list in items:
                    pos_records: list[dict[str, Any]] = [
                        json.loads(
                            pos_mm[
                                (packed >> 20) : (packed >> 20)
                                + (packed & ((1 << 20) - 1))
                            ].decode("utf-8")
                        )
                        for packed in packed_list
                    ]

                    payload_bytes = merge_pos_records_for_key(pos_records)
                    rel_off = data_f.tell()
                    data_f.write(kbytes)
                    data_f.write(payload_bytes)

                    SLOT_STRUCT.pack_into(
                        slot_table,
                        slot_cursor * SLOT_STRUCT.size,
                        h32,
                        rel_off & 0xFFFFFFFF,
                        (rel_off >> 32) & 0xFFFF,
                        len(kbytes),
                        len(payload_bytes),
                    )
                    slot_cursor += 1
                buckets[b_idx] = []

        if pos_temp_path.exists():
            pos_temp_path.unlink()

        data_len = data_temp_path.stat().st_size
        bucket_off = HEADER_STRUCT.size
        slot_off = bucket_off + len(bucket_table)
        data_off = slot_off + len(slot_table)
        expected_size = data_off + data_len + FOOTER_STRUCT.size

        lang_padded = code.encode("utf-8")[:16].ljust(16, b"\x00")
        header_bytes = HEADER_STRUCT.pack(
            KDICT_MAGIC,
            KDICT_VERSION,
            bucket_count,
            unique_key_count,
            bucket_off,
            slot_off,
            data_off,
            expected_size,
            lang_padded,
        )
        footer_bytes = FOOTER_STRUCT.pack(
            unique_key_count, data_len, KDICT_END_MAGIC
        )

        with kdict_path.open("wb") as out_f, data_temp_path.open("rb") as data_f:
            out_f.write(header_bytes)
            out_f.write(bucket_table)
            out_f.write(slot_table)
            shutil.copyfileobj(data_f, out_f, length=1024 * 1024)
            out_f.write(footer_bytes)

        if data_temp_path.exists():
            data_temp_path.unlink()

        # Verify integrity + sample word lookups
        verify_kdict_file(kdict_path, sample_keys)
        uncompressed_bytes = kdict_path.stat().st_size

        print(
            f"  [{code}] Verified {kdict_path.name} ({uncompressed_bytes / 1e6:.2f} MB). Compressing to .gz...",
            flush=True,
        )
        compressed_bytes, sha256_hex = compress_to_gzip_and_hash(kdict_path, gz_path)
        elapsed = time.time() - t0
        print(
            f"  [{code}] Done in {elapsed:.1f}s -> {gz_path.name}: {compressed_bytes / 1e6:.2f} MB compressed / {uncompressed_bytes / 1e6:.2f} MB unzipped ({unique_key_count:,} words)",
            flush=True,
        )

        now_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta = {
            "code": code,
            "name": name,
            "nativeName": lang.get("nativeName", name),
            "releaseTag": f"lang-{code}",
            "assetName": f"{code}.kdict.gz",
            "compressedBytes": compressed_bytes,
            "uncompressedBytes": uncompressed_bytes,
            "uniqueWords": unique_key_count,
            "posEntries": pos_count,
            "sha256": sha256_hex,
            "updatedAt": now_iso,
        }
        return kdict_path, gz_path, meta

    finally:
        for p in (pos_temp_path, data_temp_path):
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass


def publish_github_release(repo: str, gz_path: Path, meta: dict[str, Any]) -> str:
    tag = meta["releaseTag"]
    code = meta["code"]
    name = meta["name"]
    asset_name = meta["assetName"]
    download_url = f"https://github.com/{repo}/releases/download/{tag}/{asset_name}"

    notes = (
        f"Offline **{name} (`{code}`)** Wiktionary dictionary pack for Kanso.\n\n"
        f"- **Compressed Download (`.gz`):** `{meta['compressedBytes']:,}` bytes ({meta['compressedBytes'] / (1024 * 1024):.2f} MB)\n"
        f"- **Uncompressed On-Device (`.kdict`):** `{meta['uncompressedBytes']:,}` bytes ({meta['uncompressedBytes'] / (1024 * 1024):.2f} MB)\n"
        f"- **Unique Lookup Headwords:** `{meta['uniqueWords']:,}`\n"
        f"- **SHA-256 (`.gz`):** `{meta['sha256']}`\n"
        f"- **Updated:** `{meta['updatedAt']}`\n"
        f"- **License:** CC BY-SA 4.0 (Wiktionary contributors / Kaikki.org)\n"
    )

    view_proc = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if view_proc.returncode != 0:
        subprocess.run(
            [
                "gh",
                "release",
                "create",
                tag,
                str(gz_path),
                "--repo",
                repo,
                "--title",
                f"{name} ({code}) Offline Dictionary",
                "--notes",
                notes,
            ],
            check=True,
        )
    else:
        subprocess.run(
            [
                "gh",
                "release",
                "upload",
                tag,
                str(gz_path),
                "--clobber",
                "--repo",
                repo,
            ],
            check=True,
        )
        subprocess.run(
            [
                "gh",
                "release",
                "edit",
                tag,
                "--repo",
                repo,
                "--title",
                f"{name} ({code}) Offline Dictionary",
                "--notes",
                notes,
            ],
            check=True,
        )

    return download_url


def publish_manifest_release(repo: str) -> None:
    tag = "manifest"
    notes = (
        "Master metadata manifest (`manifest.json`) listing compressed/uncompressed byte sizes, "
        "word counts, SHA-256 checksums, and direct download URLs for all Kanso offline dictionaries."
    )
    view_proc = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if view_proc.returncode != 0:
        subprocess.run(
            [
                "gh",
                "release",
                "create",
                tag,
                str(MANIFEST_FILE),
                "--repo",
                repo,
                "--title",
                "Kanso Offline Dictionary Manifest",
                "--notes",
                notes,
            ],
            check=True,
        )
    else:
        subprocess.run(
            [
                "gh",
                "release",
                "upload",
                tag,
                str(MANIFEST_FILE),
                "--clobber",
                "--repo",
                repo,
            ],
            check=True,
        )


def load_languages() -> list[dict[str, Any]]:
    return json.loads(LANGUAGES_FILE.read_text("utf-8"))


def load_manifest(repo: str) -> dict[str, Any]:
    if MANIFEST_FILE.exists():
        data = json.loads(MANIFEST_FILE.read_text("utf-8"))
        data["repo"] = repo
        return data
    return {
        "schemaVersion": 1,
        "format": "kdict-v1",
        "repo": repo,
        "license": "CC BY-SA 4.0 (Wiktionary / Kaikki.org)",
        "updatedAt": None,
        "languages": {},
    }


def save_manifest(manifest: dict[str, Any]) -> None:
    manifest["updatedAt"] = dt.datetime.now(dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    MANIFEST_FILE.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", "utf-8"
    )


def print_language_status_table(
    languages: list[dict[str, Any]], manifest: dict[str, Any]
) -> None:
    built = manifest.get("languages", {})
    print(
        f"{'#':>2s}  {'Code':4s}  {'Language':18s}  {'Download (.gz)':>14s}  {'Unzipped':>12s}  {'Words':>10s}  {'Updated':20s}"
    )
    print("-" * 92)
    for lang in languages:
        c = lang["code"]
        m = built.get(c)
        if m:
            gz_mb = f"{m['compressedBytes'] / (1024 * 1024):.2f} MB"
            un_mb = f"{m['uncompressedBytes'] / (1024 * 1024):.2f} MB"
            words = f"{m['uniqueWords']:,}"
            upd = m.get("updatedAt", "-")
        else:
            gz_mb = "-"
            un_mb = "-"
            words = "-"
            upd = "not built"
        print(
            f"{lang['rank']:2d}  {c:4s}  {lang['name']:18s}  {gz_mb:>14s}  {un_mb:>12s}  {words:>10s}  {upd:20s}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build and publish Kanso offline Wiktionary (.kdict.gz) releases."
    )
    parser.add_argument(
        "--langs",
        nargs="+",
        help="One or more language codes or names to build/update (e.g. --langs en es ja).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Build and update all 50 configured languages.",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Build and publish only languages not yet present in manifest.json.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all 50 configured languages and their current manifest status.",
    )
    parser.add_argument(
        "--repo",
        default=DEFAULT_GITHUB_REPO,
        help=f"GitHub owner/repo for Releases (default: {DEFAULT_GITHUB_REPO}).",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Build .kdict and .kdict.gz locally without uploading to GitHub Releases.",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep generated dist/*.kdict and dist/*.kdict.gz on local disk after uploading.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Number of concurrent language workers when building multiple languages (default: 1).",
    )

    args = parser.parse_args()
    languages = load_languages()
    manifest = load_manifest(args.repo)

    if args.list:
        print_language_status_table(languages, manifest)
        return 0

    by_code = {l["code"].lower(): l for l in languages}
    by_name = {l["name"].lower(): l for l in languages}

    targets: list[dict[str, Any]] = []
    if args.all:
        targets = languages
    elif args.missing_only:
        existing = set(manifest.get("languages", {}).keys())
        targets = [l for l in languages if l["code"].lower() not in existing]
    elif args.langs:
        seen: set[str] = set()
        for tok in args.langs:
            key = tok.strip().lower()
            matched = by_code.get(key) or by_name.get(key)
            if not matched:
                print(
                    f"Error: Unknown language {tok!r}. Run with --list to see all 50 languages.",
                    file=sys.stderr,
                )
                return 1
            if matched["code"] not in seen:
                seen.add(matched["code"])
                targets.append(matched)
    else:
        parser.print_help()
        return 1

    if not targets:
        print("Nothing to build (all requested languages are already up to date).")
        return 0

    jobs = max(1, min(args.jobs, len(targets), 6))
    print(
        f"Selected {len(targets)} language(s) with {jobs} worker(s): {', '.join(t['code'] for t in targets)}",
        flush=True,
    )

    manifest_lock = threading.Lock()

    def process_one(idx_and_lang: tuple[int, dict[str, Any]]) -> None:
        idx, lang = idx_and_lang
        code = lang["code"]
        print(
            f"\n[{idx}/{len(targets)}] Processing {lang['name']} ({code})...",
            flush=True,
        )
        kdict_path, gz_path, meta = build_language_pack(lang)

        if not args.no_upload:
            print(
                f"  [{code}] Uploading {gz_path.name} to GitHub Release {meta['releaseTag']}...",
                flush=True,
            )
            download_url = publish_github_release(args.repo, gz_path, meta)
            meta["downloadUrl"] = download_url
        else:
            meta["downloadUrl"] = (
                f"https://github.com/{args.repo}/releases/download/{meta['releaseTag']}/{meta['assetName']}"
            )

        with manifest_lock:
            manifest.setdefault("languages", {})[code] = meta
            save_manifest(manifest)
            if not args.no_upload:
                publish_manifest_release(args.repo)

        if not args.keep_local and not args.no_upload:
            for p in (kdict_path, gz_path):
                if p.exists():
                    p.unlink()

    if jobs == 1:
        for item in enumerate(targets, 1):
            process_one(item)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
            futures = [
                pool.submit(process_one, item) for item in enumerate(targets, 1)
            ]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()

    print(
        "\nAll requested dictionary packs built and manifest updated successfully!",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
