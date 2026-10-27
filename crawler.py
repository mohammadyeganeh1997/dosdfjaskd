#!/usr/bin/env python3
"""Incremental archive for the publicly accessible ICBAR directory API.

One SQLite writer, bounded concurrent detail readers, resumable paginated scans.
Raw API responses and independently queryable extracted fields are retained.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import hashlib
import json
import os
import random
import subprocess
import re
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, urljoin, urlsplit

import requests

ROOT = Path(__file__).resolve().parent
DATA = Path(os.getenv('DATA_DIR', ROOT / 'data')).resolve()
DB = DATA / 'icbar.sqlite3'
ARCHIVE = DATA / 'icbar.sqlite3.gz'
SITE = os.getenv('SITE_URL', 'https://icbar.ir').rstrip('/')
API = os.getenv('API_URL', SITE + '/api/v1/directory').rstrip('/')
PAGE_SIZE = max(1, min(500, int(os.getenv('PAGE_SIZE', '100'))))
WORKERS = min(16, max(1, int(os.getenv('WORKERS', '6'))))
MAX_RPS = max(0.1, float(os.getenv('MAX_RPS', '4')))
RETRIES = max(0, int(os.getenv('RETRIES', '5')))
TIMEOUT = float(os.getenv('REQUEST_TIMEOUT', '25'))
REFRESH_DAYS = max(0, int(os.getenv('REFRESH_DAYS', '0')))
RUN_TOKEN = os.getenv('CRAWL_RUN_ID', datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
VOLATILE = {'freshness', 'matchedFields'}
IMAGE_KEYS = ('photo', 'image', 'avatar', 'picture', 'portrait', 'thumbnail', 'gallery', 'logo')
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.gif', '.avif', '.bmp'}
STOP = threading.Event()
THREAD = threading.local()
CHECKPOINTER = None
DEADLINE = None
STOP_REASON = None


def should_stop():
    global STOP_REASON
    if DEADLINE is not None and time.monotonic() >= DEADLINE and not STOP.is_set():
        STOP_REASON = 'time-budget'
        log('Run time budget reached; will checkpoint and exit.')
        STOP.set()
    return STOP.is_set()


def progress_checkpoint(cx, units=1):
    if CHECKPOINTER is not None:
        CHECKPOINTER(cx, units)


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def log(*parts):
    print(f'[{utcnow()}]', *parts, flush=True)


def json_text(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


def safe_scalar(value):
    return value if isinstance(value, (str, float, int)) and not isinstance(value, bool) else None


def field(obj, path):
    cur = obj
    for key in path.split('.'):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return safe_scalar(cur)


def fingerprint(record):
    clean = {k: v for k, v in record.items() if k not in VOLATILE}
    return hashlib.sha256(json_text(clean).encode()).hexdigest()


def check_slug(value):
    slug = str(value or '').strip()
    return slug if re.fullmatch(r'[A-Za-z0-9_-]{1,128}', slug) else None


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    if not DB.exists() and ARCHIVE.exists():
        log('Restoring compressed SQLite snapshot')
        tmp = DATA / 'icbar.sqlite3.partial'
        with gzip.open(ARCHIVE, 'rb') as src, tmp.open('wb') as dst:
            import shutil
            shutil.copyfileobj(src, dst)
        tmp.replace(DB)
    cx = sqlite3.connect(DB, timeout=30)
    cx.row_factory = sqlite3.Row
    cx.execute('PRAGMA busy_timeout=30000')
    cx.execute('PRAGMA journal_mode=WAL')
    cx.execute('PRAGMA synchronous=NORMAL')
    cx.executescript('''
        CREATE TABLE IF NOT EXISTS profiles (
            slug TEXT PRIMARY KEY,
            licence TEXT, name TEXT, latin_name TEXT, status TEXT,
            status_label TEXT, rank TEXT, professional_degree_code TEXT,
            province TEXT, city TEXT, area TEXT, since_year INTEGER,
            mobile TEXT, mobile_alt TEXT, email TEXT, website TEXT,
            photo_url TEXT, title TEXT, biography TEXT,
            practising_since TEXT, licence_issued_on TEXT, licence_expires_on TEXT,
            licence_expired INTEGER, sworn_on TEXT,
            traineeship_started_on TEXT, traineeship_ended_on TEXT,
            supervisor_name TEXT, supervisor_licence TEXT,
            office_address TEXT, office_city TEXT, office_postal TEXT,
            office_phone TEXT, office_fax TEXT, office_lat REAL, office_lng REAL,
            qualifications_json TEXT, certificates_json TEXT,
            board_history_json TEXT, commission_memberships_json TEXT,
            list_raw_json TEXT, detail_raw_json TEXT,
            list_hash TEXT, first_seen_at TEXT, last_seen_at TEXT,
            detail_fetched_at TEXT, need_detail INTEGER NOT NULL DEFAULT 1,
            detail_last_attempt_run TEXT, detail_error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_need_detail ON profiles(need_detail, detail_last_attempt_run);
        CREATE INDEX IF NOT EXISTS idx_name ON profiles(name);
        CREATE TABLE IF NOT EXISTS fields (
            slug TEXT NOT NULL, source TEXT NOT NULL,
            path TEXT NOT NULL, json_value TEXT NOT NULL,
            PRIMARY KEY(slug, source, path)
        );
        CREATE INDEX IF NOT EXISTS idx_fields_path ON fields(path);
        CREATE TABLE IF NOT EXISTS photos (
            slug TEXT NOT NULL, field_path TEXT NOT NULL, url TEXT NOT NULL,
            local_path TEXT NOT NULL,
            PRIMARY KEY(slug, field_path, url)
        );
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL, finished_at TEXT,
            status TEXT NOT NULL, next_offset INTEGER NOT NULL DEFAULT 0,
            total INTEGER, page_size INTEGER NOT NULL,
            pages INTEGER NOT NULL DEFAULT 0,
            items INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS scan_pages (
            scan_id INTEGER NOT NULL, offset INTEGER NOT NULL,
            received_at TEXT NOT NULL, raw_json TEXT NOT NULL,
            PRIMARY KEY(scan_id, offset)
        );
    ''')
    cx.commit()
    return cx


def rate_limiter(rps):
    class Gate:
        def __init__(self):
            self.lock = threading.Lock()
            self.next_at = 0.0
        def wait(self):
            with self.lock:
                now = time.monotonic()
                wait_time = max(0.0, self.next_at - now)
                self.next_at = max(now, self.next_at) + 1.0 / rps
            if wait_time:
                STOP.wait(wait_time)
    return Gate()

RATE = rate_limiter(MAX_RPS)


def fetch_json(url):
    if not hasattr(THREAD, 'session'):
        THREAD.session = requests.Session()
        THREAD.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'ICBAR-Directory-Backup/1.0 (rate-limited, public-directory)'
        })
    last_error = ''
    for attempt in range(RETRIES + 1):
        if should_stop():
            raise InterruptedError('Stop requested')
        RATE.wait()
        try:
            response = THREAD.session.get(url, timeout=TIMEOUT)
            if response.status_code in (429, 500, 502, 503, 504):
                retry_after = response.headers.get('Retry-After', '')
                pause = min(90, float(retry_after)) if retry_after.isdigit() else min(60, 2 ** attempt + random.random())
                last_error = f'HTTP {response.status_code}'
                if attempt < RETRIES:
                    STOP.wait(pause)
                    continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError('Expected JSON object')
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                code = exc.response.status_code
                if 400 <= code < 500 and code not in (408, 429):
                    break
            if attempt < RETRIES:
                STOP.wait(min(60, 2 ** attempt + random.random()))
    raise RuntimeError(f'{url}: {last_error}')


def flattened(obj, prefix='', depth=0):
    if depth > 12:
        yield prefix, json_text(obj)
    elif isinstance(obj, dict):
        if not obj:
            yield prefix, '{}'
        for key, val in obj.items():
            yield from flattened(val, f'{prefix}.{key}' if prefix else str(key), depth + 1)
    elif isinstance(obj, list):
        yield prefix, json_text(obj)
        for index, val in enumerate(obj[:100]):
            if isinstance(val, (dict, list)):
                yield from flattened(val, f'{prefix}[{index}]', depth + 1)
    else:
        yield prefix, json_text(obj)


def images(obj):
    origin = urlsplit(SITE)
    seen = set()
    def walk(node, path='', context=False, depth=0):
        if depth > 12:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                p = f'{path}.{key}' if path else str(key)
                yield from walk(value, p, context or any(k in str(key).lower() for k in IMAGE_KEYS), depth + 1)
        elif isinstance(node, list):
            for i, value in enumerate(node[:200]):
                yield from walk(value, f'{path}[{i}]', context, depth + 1)
        elif context and isinstance(node, str) and node.strip():
            raw = node.strip()
            if raw.startswith('//'):
                raw = origin.scheme + ':' + raw
            url = urljoin(SITE + '/', raw)
            parsed = urlsplit(url)
            if parsed.scheme not in ('http', 'https') or parsed.netloc.lower() != origin.netloc.lower():
                return
            ext = Path(parsed.path.lower()).suffix
            if ext not in IMAGE_EXTS and not (parsed.path.startswith('/media/') or parsed.path.startswith('/uploads/')):
                return
            if (path, url) not in seen:
                seen.add((path, url))
                yield path, url
    yield from walk(obj)


def save_images(cx, slug, obj):
    for source, url in images(obj):
        parsed = urlsplit(url)
        ext = Path(parsed.path.lower()).suffix
        if ext not in IMAGE_EXTS:
            ext = '.jpg'
        name = hashlib.sha256(url.encode()).hexdigest()[:24] + ext
        relative = f'photos/{slug}/{name}'
        cx.execute('INSERT OR IGNORE INTO photos(slug,field_path,url,local_path) VALUES(?,?,?,?)',
                   (slug, source, url, relative))


def upsert_list(cx, record, ts):
    slug = check_slug(record.get('slug') or record.get('licence'))
    if not slug:
        return 0, 0
    old = cx.execute('SELECT list_hash FROM profiles WHERE slug=?', (slug,)).fetchone()
    fp = fingerprint(record)
    changed = bool(old and old['list_hash'] != fp)
    row = {
        'licence': field(record, 'licence'),
        'name': field(record, 'name'),
        'latin_name': field(record, 'latinName'),
        'status': field(record, 'status'),
        'status_label': field(record, 'statusLabel'),
        'rank': field(record, 'rank'),
        'professional_degree_code': field(record, 'professionalDegreeCode'),
        'province': field(record, 'province'),
        'city': field(record, 'city'),
        'area': field(record, 'area'),
        'since_year': field(record, 'since'),
        'photo_url': field(record, 'photo'),
        'list_raw_json': json_text(record),
        'list_hash': fp,
        'last_seen_at': ts,
    }
    if old is None:
        cols = ['slug', 'first_seen_at', *row.keys()]
        vals = [slug, ts, *row.values()]
        cx.execute(f"INSERT INTO profiles({','.join(cols)}) VALUES({','.join('?' for _ in vals)})", vals)
    else:
        assignments = ','.join(f'{k}=?' for k in row)
        cx.execute(f'UPDATE profiles SET {assignments}, need_detail=CASE WHEN ? THEN 1 ELSE need_detail END WHERE slug=?',
                   (*row.values(), int(changed), slug))
    if old is None or changed:
        cx.execute('DELETE FROM fields WHERE slug=? AND source=?', (slug, 'list'))
        cx.executemany('INSERT INTO fields(slug,source,path,json_value) VALUES(?,?,?,?)',
                       ((slug, 'list', path, value) for path, value in flattened(record) if path))
    save_images(cx, slug, record)
    return int(old is None), int(changed)


def store_detail(cx, slug, detail):
    if not isinstance(detail, dict):
        raise ValueError('Detail is not an object')
    reply_slug = check_slug(detail.get('slug') or detail.get('licence'))
    if reply_slug and reply_slug != slug:
        raise ValueError(f'Detail slug mismatch expected={slug} actual={reply_slug}')
    mapping = {
        'licence': 'licence', 'name': 'name', 'latin_name': 'latinName',
        'status': 'status', 'status_label': 'statusLabel', 'rank': 'rank',
        'professional_degree_code': 'professionalDegreeCode',
        'province': 'province', 'city': 'city', 'area': 'area',
        'since_year': 'since', 'mobile': 'mobile', 'mobile_alt': 'mobileAlt',
        'email': 'email', 'website': 'website', 'photo_url': 'photo',
        'title': 'title', 'biography': 'biography',
        'practising_since': 'practisingSince', 'licence_issued_on': 'licenceIssuedOn',
        'licence_expires_on': 'licenceExpiresOn', 'sworn_on': 'swornOn',
        'traineeship_started_on': 'traineeshipStartedOn',
        'traineeship_ended_on': 'traineeshipEndedOn',
        'supervisor_name': 'supervisor.name', 'supervisor_licence': 'supervisor.licence',
        'office_address': 'office.address', 'office_city': 'office.city',
        'office_postal': 'office.postal', 'office_phone': 'office.phone',
        'office_fax': 'office.fax', 'office_lat': 'office.lat', 'office_lng': 'office.lng'
    }
    columns = {col: field(detail, src) for col, src in mapping.items()}
    if isinstance(detail.get('licenceExpired'), bool):
        columns['licence_expired'] = int(detail['licenceExpired'])
    for col, key in [('qualifications_json', 'qualifications'), ('certificates_json', 'certificates'),
                     ('board_history_json', 'boardHistory'), ('commission_memberships_json', 'commissionMemberships')]:
        if key in detail:
            columns[col] = json_text(detail[key])
    updates = [f'{k}=COALESCE(?,{k})' for k in columns]
    cx.execute(f'''UPDATE profiles SET {','.join(updates)}, detail_raw_json=?, detail_fetched_at=?,
                need_detail=0, detail_last_attempt_run=NULL, detail_error=NULL WHERE slug=?''',
               (*columns.values(), json_text(detail), utcnow(), slug))
    cx.execute('DELETE FROM fields WHERE slug=? AND source=?', (slug, 'detail'))
    cx.executemany('INSERT INTO fields(slug,source,path,json_value) VALUES(?,?,?,?)',
                   ((slug, 'detail', path, value) for path, value in flattened(detail) if path))
    save_images(cx, slug, detail)


def active_scan(cx):
    return cx.execute("SELECT * FROM scans WHERE status IN ('running','paused') ORDER BY id DESC LIMIT 1").fetchone()


def scan(cx, max_pages=0):
    current = active_scan(cx)
    if current:
        sid, offset = current['id'], current['next_offset']
        cx.execute("UPDATE scans SET status='running' WHERE id=?", (sid,))
        cx.commit()
        log('Resuming listing scan', sid, 'at offset', offset)
    else:
        cur = cx.execute("INSERT INTO scans(started_at,status,page_size) VALUES(?,'running',?)", (utcnow(), PAGE_SIZE))
        sid, offset = cur.lastrowid, 0
        cx.commit()
        log('Starting new listing scan', sid)
    pages = 0
    try:
        while not should_stop():
            if max_pages and pages >= max_pages:
                break
            url = f'{API}/?q=&searched=1&limit={PAGE_SIZE}&offset={offset}'
            payload = fetch_json(url)
            records = payload.get('results')
            if not isinstance(records, list):
                raise ValueError(f'Unexpected list JSON at offset {offset}: missing results array')
            total = payload.get('total')
            total = int(total) if total is not None else None
            reported_offset = payload.get('offset')
            if reported_offset is not None and int(reported_offset) != offset:
                raise ValueError(f'API offset mismatch: wanted {offset} got {reported_offset}')
            if not records and total is not None and offset < total:
                raise ValueError('Unexpected empty page before the advertised total')
            if not records and total is None:
                done = True
            else:
                done = bool((total is not None and offset + len(records) >= total)
                            or (total is None and len(records) < PAGE_SIZE))
            ts = utcnow()
            new = changed = 0
            with cx:
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    a, b = upsert_list(cx, record, ts)
                    new += a
                    changed += b
                cx.execute('INSERT OR REPLACE INTO scan_pages(scan_id,offset,received_at,raw_json) VALUES(?,?,?,?)',
                           (sid, offset, ts, json_text(payload)))
                offset += len(records)
                cx.execute('UPDATE scans SET next_offset=?,total=?,pages=pages+1,items=items+? WHERE id=?',
                           (offset, total, len(records), sid))
                if done:
                    cx.execute("UPDATE scans SET status='completed', finished_at=? WHERE id=?", (utcnow(), sid))
            pages += 1
            progress_checkpoint(cx, max(1, len(records)))
            if pages == 1 or pages % 20 == 0 or new or changed or done:
                log(f'list offset={offset} total={total} new={new} changed={changed}')
            if done:
                log('Listing complete')
                return True
    finally:
        cx.execute("UPDATE scans SET status='paused' WHERE status='running'")
        cx.commit()
    return False


def refresh_stale(cx):
    if not REFRESH_DAYS:
        return
    cutoff = (datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)).isoformat(timespec='seconds')
    with cx:
        cur = cx.execute('''UPDATE profiles SET need_detail=1 WHERE detail_raw_json IS NOT NULL
                            AND (detail_fetched_at IS NULL OR detail_fetched_at < ?)''', (cutoff,))
    log('Old profiles scheduled for refresh:', cur.rowcount)


def download_detail(slug):
    try:
        data = fetch_json(f'{API}/{quote(slug, safe="")}')
        if not isinstance(data, dict):
            raise ValueError('Not a detail object')
        return slug, data, None
    except Exception as exc:
        return slug, None, str(exc)[:500]


def pending_slugs(cx, max_details):
    sql = '''SELECT slug FROM profiles WHERE (need_detail=1 OR detail_raw_json IS NULL)
             AND (detail_last_attempt_run IS NULL OR detail_last_attempt_run<>?)
             ORDER BY (detail_raw_json IS NOT NULL), slug'''
    if max_details:
        return [r[0] for r in cx.execute(sql + ' LIMIT ?', (RUN_TOKEN, max_details))]
    return [r[0] for r in cx.execute(sql, (RUN_TOKEN,))]


def details(cx, max_details=0, max_seconds=0):
    refresh_stale(cx)
    slugs = iter(pending_slugs(cx, max_details))
    started = time.monotonic()
    ok = failed = 0
    futures = {}
    def submit(pool):
        if should_stop() or (max_seconds and time.monotonic()-started > max_seconds):
            return False
        try:
            slug = next(slugs)
        except StopIteration:
            return False
        futures[pool.submit(download_detail, slug)] = slug
        return True
    with cf.ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix='icbar') as pool:
        for _ in range(WORKERS * 2):
            if not submit(pool):
                break
        while futures:
            completed, _ = cf.wait(futures, timeout=1.0, return_when=cf.FIRST_COMPLETED)
            if not completed:
                if should_stop():
                    # Permit active requests to exit via STOP, but do not enqueue more.
                    continue
                continue
            with cx:
                for future in completed:
                    slug = futures.pop(future)
                    try:
                        _, payload, error = future.result()
                    except Exception as exc:
                        payload, error = None, str(exc)
                    if error:
                        cx.execute('UPDATE profiles SET detail_last_attempt_run=?,detail_error=? WHERE slug=?',
                                   (RUN_TOKEN, error, slug))
                        failed += 1
                    else:
                        try:
                            store_detail(cx, slug, payload)
                            ok += 1
                        except Exception as exc:
                            cx.execute('UPDATE profiles SET detail_last_attempt_run=?,detail_error=? WHERE slug=?',
                                       (RUN_TOKEN, str(exc)[:500], slug))
                            failed += 1
            progress_checkpoint(cx, len(completed))
            if (ok + failed) % 100 < len(completed) or not futures:
                log(f'details done={ok} failed={failed} workers={WORKERS}')
            for _ in completed:
                submit(pool)
    return ok, failed


def write_photo_downloader():
    # Generate a stand-alone Bash script with URL entries stored as plain text.
    header = (ROOT / 'scripts' / 'download_photos_header.sh').read_text(encoding='utf-8')
    footer = (ROOT / 'scripts' / 'download_photos_footer.sh').read_text(encoding='utf-8')
    output = ROOT / 'download_photos.sh'
    temp = ROOT / 'download_photos.sh.tmp'
    with temp.open('w', encoding='utf-8', newline='\n') as f:
        f.write(header)
        with (DATA / 'photos.tsv').open(encoding='utf-8') as mf:
            next(mf, None)
            for raw in mf:
                cells = raw.rstrip('\n').split('\t')
                if len(cells) != 4:
                    continue
                slug, url, local_path, _field_path = cells
                if not check_slug(slug) or not url.startswith(SITE + '/'):
                    continue
                if not local_path.startswith(f'photos/{slug}/'):
                    continue
                f.write('\t'.join((slug, url, local_path)) + '\n')
        f.write(footer)
    temp.chmod(0o755)
    temp.replace(output)


def export(cx):
    DATA.mkdir(parents=True, exist_ok=True)
    sql = ('SELECT p.slug,p.url,p.local_path,MIN(p.field_path) '
           'FROM photos AS p GROUP BY p.slug,p.url,p.local_path '
           'ORDER BY p.slug,p.local_path')
    temp = DATA / 'photos.tsv.tmp'
    with temp.open('w', encoding='utf-8', newline='\n') as fh:
        fh.write('slug\turl\tlocal_path\tfield_path\n')
        for row in cx.execute(sql):
            safe = [str(x).replace('\t', '%09').replace('\r', '%0D').replace('\n', '%0A') for x in row]
            fh.write('\t'.join(safe) + '\n')
    temp.replace(DATA / 'photos.tsv')
    write_photo_downloader()
    metrics = dict(cx.execute(
        "SELECT 'profiles' AS k, COUNT(*) AS v FROM profiles "
        "UNION ALL SELECT 'details',COUNT(*) FROM profiles WHERE detail_raw_json IS NOT NULL "
        "UNION ALL SELECT 'pending',COUNT(*) FROM profiles WHERE need_detail=1 OR detail_raw_json IS NULL "
        "UNION ALL SELECT 'photos',COUNT(*) FROM photos"
    ))
    state = cx.execute('SELECT status,next_offset,total,finished_at FROM scans ORDER BY id DESC LIMIT 1').fetchone()
    report = {
        'generated_at': utcnow(), 'counts': metrics,
        'last_listing_scan': dict(state) if state is not None else None,
        'db_archive': 'data/icbar.sqlite3.gz',
        'photo_downloader': 'download_photos.sh',
        'images': 'data/photos.tsv'
    }
    (DATA / 'summary.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    log('Exported photo manifest, downloader and counts:', metrics)
    return metrics


def snapshot(cx):
    # Create consistent SQLite snapshot via SQLite online backup API; gzip avoids GitHub 100MB file cap.
    temp_db = DATA / '.snapshot.sqlite3'
    temp_gz = DATA / 'icbar.sqlite3.gz.tmp'
    try:
        backup = sqlite3.connect(temp_db)
        cx.backup(backup)
        backup.close()
        with temp_db.open('rb') as inp, gzip.open(temp_gz, 'wb', compresslevel=6) as out:
            import shutil
            shutil.copyfileobj(inp, out, 1024 * 1024)
        size = temp_gz.stat().st_size
        if size > 95_000_000:
            raise RuntimeError(f'Compressed DB is {size/1e6:.1f}MB; exceeds GitHub single-file safety limit. Use release/artifact or split archive.')
        temp_gz.replace(ARCHIVE)
        log('Saved compressed SQLite:', ARCHIVE.name, f'{size/1048576:.2f}MiB')
    finally:
        temp_db.unlink(missing_ok=True)
        temp_gz.unlink(missing_ok=True)


def status(cx):
    export(cx)
    print((DATA / 'summary.json').read_text(encoding='utf-8'))


class CheckpointManager:
    def __init__(self, count, seconds, enabled):
        self.count_limit = max(1, count)
        self.seconds_limit = max(1, seconds)
        self.enabled = enabled
        self.dirty = 0
        self.last = time.monotonic()

    def __call__(self, cx, units=0, force=False):
        self.dirty += units
        if not force and self.dirty < self.count_limit and time.monotonic()-self.last < self.seconds_limit:
            return
        export(cx)
        snapshot(cx)
        if self.enabled:
            subprocess.run(['bash', str(ROOT / 'scripts' / 'checkpoint.sh')], cwd=ROOT, check=True)
        self.dirty = 0
        self.last = time.monotonic()


def main():
    global CHECKPOINTER, DEADLINE, STOP_REASON
    parser = argparse.ArgumentParser(description='ICBAR GitHub Actions resumable crawler')
    parser.add_argument('command', choices=['scan', 'details', 'sync', 'export', 'status', 'snapshot'])
    parser.add_argument('--max-pages', type=int, default=0)
    parser.add_argument('--max-details', type=int, default=0)
    parser.add_argument('--max-seconds', type=int, default=0, help='Detail phase budget')
    parser.add_argument('--run-seconds', type=int, default=0, help='Whole run budget; graceful checkpoint')
    parser.add_argument('--checkpoint-records', type=int, default=1200)
    parser.add_argument('--checkpoint-seconds', type=int, default=420)
    parser.add_argument('--git-checkpoint', action='store_true', help='Push checkpoints to the current Git branch')
    opts = parser.parse_args()
    if opts.run_seconds:
        DEADLINE = time.monotonic() + opts.run_seconds

    def request_stop(signum, frame):
        global STOP_REASON
        STOP_REASON = 'signal'
        log('Signal received; checkpointing completed records')
        STOP.set()
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    cx = connect()
    CHECKPOINTER = CheckpointManager(opts.checkpoint_records, opts.checkpoint_seconds, opts.git_checkpoint)
    raised = None
    try:
        if opts.command in ('scan', 'sync'):
            scan(cx, opts.max_pages)
        if opts.command in ('details', 'sync') and not should_stop():
            details(cx, opts.max_details, opts.max_seconds)
    except Exception as exc:
        log('Crawler error; preserving committed progress:', exc)
        raised = exc
    finally:
        try:
            if opts.command in ('scan','details','sync','snapshot','export','status'):
                CHECKPOINTER(cx, force=True)
            if opts.command == 'status':
                print((DATA / 'summary.json').read_text(encoding='utf-8'))
        finally:
            cx.close()
    if raised is not None:
        raise raised
    return 130 if STOP_REASON == 'signal' else 0

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        log('ERROR:', exc)
        sys.exit(1)
