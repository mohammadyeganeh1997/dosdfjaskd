"""Test a resume + delta crawl against a local fake HTTP API."""
import gzip
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[1]
ITEMS = [
    {'slug':'1001','licence':'1001','name':'alpha','photo':'/media/a.png','freshness':{'ageHours':1}},
    {'slug':'1002','licence':'1002','name':'beta','photo':'/media/b.png'},
    {'slug':'1003','licence':'1003','name':'gamma','photo':'/media/c.png'},
]
counts = {}
PNG = bytes.fromhex('89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000b49444154789c636000020000050001a5f645400000000049454e44ae426082')

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        counts[url.path] = counts.get(url.path,0) + 1
        if url.path in ('/api/v1/directory','/api/v1/directory/'):
            query = parse_qs(url.query)
            off = int(query.get('offset',['0'])[0]); lim = int(query.get('limit',['2'])[0])
            res = {'results':ITEMS[off:off+lim], 'total':len(ITEMS),'offset':off}
            self.reply(json.dumps(res).encode()); return
        if url.path.startswith('/api/v1/directory/'):
            slug = url.path.rsplit('/',1)[-1]
            match = next((x for x in ITEMS if x['slug']==slug),None)
            if match:
                detail = dict(match, mobile='09123456789', office={'address':'تهران', 'lat': 35.7}, qualifications=[{'title':'LLM'}])
                self.reply(json.dumps(detail).encode()); return
        if url.path.startswith('/media/'):
            self.send_response(200);self.send_header('Content-Type','image/png');self.send_header('Content-Length',str(len(PNG)))
            self.end_headers();self.wfile.write(PNG);return
        self.send_error(404)
    def reply(self,body):
        self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)))
        self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass


def run(args,env,success=True):
    p = subprocess.run([sys.executable,str(ROOT/'crawler.py'),*args], env=env, text=True, capture_output=True)
    print(p.stdout[-800:])
    if p.returncode!=0 and success:
        print(p.stderr)
        raise AssertionError('crawler failed')
    return p


def main():
    srv=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    t=threading.Thread(target=srv.serve_forever,daemon=True);t.start()
    with tempfile.TemporaryDirectory() as tmp:
        env={**os.environ,'SITE_URL':f'http://127.0.0.1:{srv.server_address[1]}',
             'API_URL':f'http://127.0.0.1:{srv.server_address[1]}/api/v1/directory',
             'DATA_DIR':tmp,'PAGE_SIZE':'2','MAX_RPS':'200','WORKERS':'2'}
        try:
            run(['sync','--max-pages','1','--max-details','2','--checkpoint-records','1'],env)
            cx = sqlite3.connect(Path(tmp)/'icbar.sqlite3')
            assert cx.execute('SELECT COUNT(*) FROM profiles').fetchone()[0]==2
            assert cx.execute("SELECT status,next_offset FROM scans").fetchone()==('paused',2)
            cx.close()
            (Path(tmp)/'icbar.sqlite3').unlink()  # fresh runner restores from Git snapshot
            run(['sync','--checkpoint-records','2'],env)
            first={k:v for k,v in counts.items() if k.startswith('/api/v1/directory/')}
            assert first['/api/v1/directory/1001']==1
            assert first['/api/v1/directory/1002']==1
            assert first['/api/v1/directory/1003']==1
            assert (ROOT/'download_photos.sh').exists()
            contents=(ROOT/'download_photos.sh').read_text()
            assert '/media/a.png' in contents and '/media/c.png' in contents
            subprocess.run(['bash','-n',str(ROOT/'download_photos.sh')],check=True)
            p=subprocess.run(['bash',str(ROOT/'download_photos.sh'),'--count'],text=True,capture_output=True,check=True)
            assert p.stdout.strip()=='3',p.stdout
            ITEMS.insert(0,{'slug':'1004','licence':'1004','name':'delta','photo':'/media/d.png'})
            run(['sync','--checkpoint-records','2'],env)
            for slug in ('1001','1002','1003'):
                assert counts[f'/api/v1/directory/{slug}']==1,counts
            assert counts['/api/v1/directory/1004']==1
            db=sqlite3.connect(Path(tmp)/'icbar.sqlite3')
            assert db.execute('SELECT COUNT(*) FROM profiles').fetchone()[0]==4
            assert db.execute('SELECT COUNT(*) FROM profiles WHERE detail_raw_json IS NOT NULL').fetchone()[0]==4
            assert db.execute('SELECT COUNT(*) FROM fields WHERE source="detail" AND path="office.address"').fetchone()[0]==4
            assert db.execute('SELECT COUNT(*) FROM fields WHERE source="list" AND path="name"').fetchone()[0]==4
            assert db.execute('SELECT COUNT(*) FROM photos').fetchone()[0]==4
            db.close()
            with gzip.open(Path(tmp)/'icbar.sqlite3.gz','rb') as g:
                assert g.read(16).startswith(b'SQLite format 3')
            # Stand-alone download script, with fake server on loopback.
            out=Path(tmp)/'saved_images'
            env_download={**env, 'OUTPUT_DIR':str(out),'JOBS':'3','DELAY':'0'}
            p = subprocess.run(['bash',str(ROOT/'download_photos.sh')],env=env_download,capture_output=True,text=True)
            assert p.returncode==0,(p.stdout,p.stderr)
            assert len(list(out.rglob('*.png')))==4
            old_counts = dict(counts)
            p = subprocess.run(['bash',str(ROOT/'download_photos.sh')],env=env_download,capture_output=True,text=True)
            assert p.returncode==0,(p.stdout,p.stderr)
            for key in old_counts:
                if key.startswith('/media/'):
                    assert counts[key]==old_counts[key]
            print('PASS: resumable scans, change detection, raw JSON, SQL fields, photos, self-contained Bash')
        finally:
            srv.shutdown()

if __name__=='__main__':main()
