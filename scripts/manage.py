#!/usr/bin/env python3
"""Explicit schema operations. This module never inserts application content."""
import argparse,base64,json,os,shlex,sqlite3,subprocess,urllib.error,urllib.parse,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def values(path):
    data={}
    for line in path.read_text().splitlines():
        line=line.strip()
        if not line or line.startswith('#') or '=' not in line:continue
        key,value=line.split('=',1)
        tokens=shlex.split(value,comments=True)
        data[key.strip()]=tokens[0] if tokens else ''
    return data

def couch(config,method,path,body=None):
    data=json.dumps(body).encode() if body is not None else None
    token=base64.b64encode((config.get('COUCHDB_USER','')+':'+config.get('COUCHDB_PASSWORD','')).encode()).decode()
    req=urllib.request.Request(config['COUCHDB_URL'].rstrip('/')+'/'+path,data=data,method=method,headers={'Authorization':'Basic '+token,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=15) as response:return response.status
    except urllib.error.HTTPError as error:
        if method=='PUT' and error.code==412:return error.code
        raise SystemExit(f'CouchDB operation failed: HTTP {error.code}') from None

def migrate(config,apply):
    target=urllib.parse.urlparse(config['DATABASE_URL'])
    files=sorted((ROOT/'migrations').glob('*.sql'))
    name=target.path.lstrip('/') if target.scheme!='sqlite' else target.path
    print('Schema target:',target.hostname or 'local SQLite',name)
    print('Schema files:',', '.join(f.name for f in files))
    print('CouchDB database:',config.get('COUCHDB_DATABASE','annotations_documents'))
    if not apply:
        print('Dry run: nothing changed.');return
    if target.scheme=='sqlite':
        path=Path(target.path)
        if not str(path).startswith('/tmp/'):
            raise SystemExit('SQLite migrations require an explicit isolated /tmp test target.')
        with sqlite3.connect(path) as conn:
            conn.execute('PRAGMA foreign_keys=ON')
            for file in files:conn.executescript(file.read_text())
        return
    if target.scheme not in ('postgres','postgresql') or name!='asodya_annotations':
        raise SystemExit('Refusing schema migration: expected the dedicated asodya_annotations PostgreSQL database.')
    env=dict(os.environ,PGHOST=target.hostname or '',PGPORT=str(target.port or 5432),PGUSER=urllib.parse.unquote(target.username or ''),PGPASSWORD=urllib.parse.unquote(target.password or ''),PGDATABASE=name,PGSSLMODE=dict(urllib.parse.parse_qsl(target.query)).get('sslmode','prefer'))
    for file in files:
        subprocess.run(['psql','-X','-q','-v','ON_ERROR_STOP=1','--single-transaction','-f',str(file)],env=env,check=True)
    database=config.get('COUCHDB_DATABASE','annotations_documents')
    if database!='annotations_documents':raise SystemExit('Expected the dedicated annotations_documents database.')
    couch(config,'PUT',database)
    couch(config,'POST',database+'/_index',{'index':{'fields':['type','post_id','language']},'name':'annotations-translations','type':'json'})
    print('Schema migration complete. No content was populated.')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--yes',action='store_true')
    parser.add_argument('--dry-run',action='store_true')
    args=parser.parse_args()
    if not args.yes and not args.dry_run:parser.error('Choose --dry-run or --yes.')
    path=ROOT/'.env'
    if not path.is_file():raise SystemExit('Create the canonical .env from .env.example before running migrations.')
    migrate(values(path),args.yes)
if __name__=='__main__':main()
