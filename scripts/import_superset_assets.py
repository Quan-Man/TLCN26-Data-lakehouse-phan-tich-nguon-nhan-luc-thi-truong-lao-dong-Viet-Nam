#!/usr/bin/env python3
"""Import a real Superset 4.1.2 dashboard ZIP without overwriting user assets.

Run INSIDE the configured Superset container. A single Trino database in the
export is rebound in memory to SUPERSET_TRINO_SQLALCHEMY_URI; the ZIP is never
rewritten and must not contain secrets. The Superset 4.1.2 import_dashboards
CLI hardcodes overwrite=True; this script invokes its importer with False.

By default a conflicting dashboard/chart/dataset UUID stops the entire import.
--auto is opt-in: it does nothing unless SUPERSET_AUTO_IMPORT_DASHBOARDS=true;
then conflicts skip the whole bundle without changing user edits. For separate
Superset writers, stop concurrent asset edits/imports during the operation.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from uuid import UUID
from zipfile import BadZipFile, ZipFile

import yaml
from sqlalchemy.engine import URL, make_url

EXPECTED_SUPERSET = '4.1.2'
EXPORT_FORMAT = '1.0.0'
KINDS = ('databases', 'datasets', 'charts', 'dashboards')
SECRET_KEY = re.compile(r'(^|_)(password|passwd|secret|private_key|token|access_token|access_key|api_key|credential|credentials|encrypted_extra)(_|$)', re.I)
MASKED = {'', 'xxxxxxxxxx', '********', '***', 'null', '{}'}


class ImportSafetyError(Exception):
    """Public message never contains connection strings or secret values."""


def is_empty_or_masked(value):
    if value is None or value is False: return True
    if isinstance(value,str): return value.strip().lower() in MASKED
    if isinstance(value,(dict,list,tuple)): return not value
    return False


def inspect_secrets(value, seen=None):
    """Conservative detector; a human must still review arbitrary chart SQL."""
    seen = seen or set()
    if isinstance(value,(dict,list)):
        if id(value) in seen: return
        seen.add(id(value))
    if isinstance(value,dict):
        for key,item in value.items():
            if isinstance(key,str) and SECRET_KEY.search(key) and not is_empty_or_masked(item):
                raise ImportSafetyError('Bundle contains a nonempty sensitive field')
            inspect_secrets(item,seen)
    elif isinstance(value,list):
        for item in value: inspect_secrets(item,seen)
    elif isinstance(value,str):
        if '-----BEGIN ' in value and 'PRIVATE KEY-----' in value:
            raise ImportSafetyError('Bundle contains private-key material')
        if re.search(r'(?i)\b[a-z][a-z0-9+.-]*://[^\s/@:]+:[^\s/@]+@',value):
            raise ImportSafetyError('Bundle contains a URI with embedded credentials')
        # Superset often stores JSON inside YAML strings (e.g. `extra` and
        # chart params). Walk that structure too, without printing its contents.
        stripped = value.strip()
        if stripped.startswith(('{','[')):
            try: nested = json.loads(stripped)
            except (TypeError,ValueError,RecursionError): nested = None
            if nested is not None: inspect_secrets(nested,seen)
        if re.search(r'(?i)\b(?:password|passwd|client_secret|access_token|api_key|credential)[\'\"]?\s*[:=]\s*[\'\"]?[^\s\'\"$}{,]+',value):
            raise ImportSafetyError('Bundle contains a possible inline credential')


def safe_url(raw, *, bundle=False):
    try:
        url=make_url(raw)
    except Exception:
        raise ImportSafetyError('Invalid Trino SQLAlchemy URI') from None
    if url.drivername not in ('trino','trino+http','trino+https') or not url.host or not url.database:
        raise ImportSafetyError('Expected a Trino URI with host and catalog/schema')
    if bundle and url.password:
        raise ImportSafetyError('Bundle database URI must not contain a password')
    for key,value in url.query.items():
        if SECRET_KEY.search(key) and not is_empty_or_masked(value):
            raise ImportSafetyError('URI query must not contain secret parameters')
    inspect_secrets(dict(url.query))
    return url


def public_url(url):
    # URL.set(password=None) can retain the prior password in SQLAlchemy 1.4.
    return URL.create(url.drivername,username=url.username,password=None,host=url.host,
        port=url.port,database=url.database,query=url.query)


def endpoint_identity(raw):
    url=safe_url(raw)
    return (url.drivername,url.username,url.host,url.port,url.database,
            tuple(sorted((k,tuple(v) if isinstance(v,tuple) else v) for k,v in url.query.items())))


def read_bundle(path):
    path=Path(path)
    if not path.is_file(): raise ImportSafetyError('Dashboard bundle does not exist')
    if path.stat().st_size > 50*1024*1024: raise ImportSafetyError('Bundle exceeds 50 MiB')
    contents={}; seen=set(); roots=set(); ids={kind:{} for kind in KINDS}
    try:
        with ZipFile(path) as bundle:
            entries=bundle.infolist()
            if len(entries)>4000 or sum(x.file_size for x in entries)>100*1024*1024:
                raise ImportSafetyError('Bundle has too many entries or is too large')
            for info in entries:
                name=info.filename
                if '\\' in name or name.startswith('/') or '..' in PurePosixPath(name).parts:
                    raise ImportSafetyError('Unsafe bundle path')
                if ((info.external_attr >> 16) & 0o170000)==stat.S_IFLNK:
                    raise ImportSafetyError('Symbolic links are not permitted in bundle')
                if info.is_dir(): continue
                path_parts=PurePosixPath(name).parts
                if len(path_parts)<2 or path_parts[0] in ('','.'):raise ImportSafetyError('Bundle must have one export root folder')
                roots.add(path_parts[0]);rel='/'.join(path_parts[1:])
                if rel in seen:raise ImportSafetyError('Duplicate bundle path')
                seen.add(rel)
                if info.file_size and info.file_size / max(info.compress_size,1) > 200:
                    raise ImportSafetyError('Bundle contains an excessive compression ratio')
                if not (rel=='metadata.yaml' or (rel.split('/')[0] in KINDS and rel.lower().endswith(('.yaml','.yml')))):
                    raise ImportSafetyError('Bundle must contain only dashboard export YAML files')
                text=bundle.read(info).decode('utf-8');contents[rel]=text
                try: obj=yaml.safe_load(text)
                except yaml.YAMLError:raise ImportSafetyError('Invalid YAML in bundle') from None
                if not isinstance(obj,dict):raise ImportSafetyError('Invalid YAML asset')
                inspect_secrets(obj)
                if rel!='metadata.yaml':
                    kind=rel.split('/')[0]
                    try:identifier=str(UUID(str(obj.get('uuid'))))
                    except (TypeError,ValueError):raise ImportSafetyError(f'Invalid UUID in {kind}') from None
                    if identifier in ids[kind]:raise ImportSafetyError(f'Duplicate {kind} UUID in bundle')
                    ids[kind][identifier]=(rel,obj)
    except (BadZipFile,UnicodeError,OSError):
        raise ImportSafetyError('Cannot read a valid UTF-8 Superset ZIP') from None
    if len(roots)!=1 or 'metadata.yaml' not in contents:raise ImportSafetyError('Missing metadata or multiple export roots')
    metadata=yaml.safe_load(contents['metadata.yaml'])
    if str(metadata.get('version'))!=EXPORT_FORMAT or metadata.get('type')!='Dashboard':
        raise ImportSafetyError('Expected a Superset dashboard export format 1.0.0')
    if len(ids['databases'])!=1 or any(not ids[k] for k in ('datasets','charts','dashboards')):
        raise ImportSafetyError('Expected one Trino database and at least one dataset, chart and dashboard')
    path_db,dbconfig=next(iter(ids['databases'].values()))
    if not isinstance(dbconfig.get('database_name'),str) or not dbconfig['database_name'].strip():
        raise ImportSafetyError('Bundle database must have a name')
    safe_url(dbconfig.get('sqlalchemy_uri'),bundle=True)
    if any((c.get('viz_type')=='filter_box') for _,c in ids['charts'].values()):
        # Superset 4.1.2 migrates and deletes filter_box charts while importing.
        raise ImportSafetyError('Migrate filter_box charts before export/import')
    return contents,ids,path_db


def prepare_contents(contents,ids,db_path,trino_uri):
    uri=safe_url(trino_uri)
    config=next(iter(ids['databases'].values()))[1].copy()
    config['sqlalchemy_uri']=public_url(uri).render_as_string(hide_password=False)
    # A masked password in the archive is never forwarded as a real secret.
    config.pop('password',None)
    result=dict(contents)
    result[db_path]=yaml.safe_dump(config,allow_unicode=True,sort_keys=False)
    passwords={db_path:uri.password} if uri.password else {}
    return result,passwords,endpoint_identity(trino_uri)


class SupersetBackend:
    """Adapter over the 4.1.2 application; all calls occur in Flask app context."""
    def __init__(self, username):
        from flask import g
        from superset import db, security_manager
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice
        from superset.connectors.sqla.models import SqlaTable
        self.db=db
        self.classes={'databases':Database,'datasets':SqlaTable,'charts':Slice,'dashboards':Dashboard}
        user=security_manager.find_user(username=username)
        if user is None:raise ImportSafetyError('Superset import user does not exist')
        g.user=user

    def find_uuid(self,kind,identifier):
        return self.db.session.query(self.classes[kind]).filter_by(uuid=UUID(identifier)).first()

    def find_database_name(self,name):
        return self.db.session.query(self.classes['databases']).filter_by(database_name=name).first()

    def import_bundle(self,contents,passwords):
        from sqlalchemy import event
        from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
        model_types=tuple(self.classes.values());new_ids=set()
        # Protect existing objects, including unexpected deletion in a race.
        # The transaction aborts instead of mutating a pre-existing asset.
        def before_flush(session,flush_context,instances):
            for obj in session.new:
                if isinstance(obj,model_types):new_ids.add(id(obj))
            for obj in session.deleted:
                if isinstance(obj,model_types):raise ImportSafetyError('Import attempted to delete an asset')
            for obj in session.dirty:
                if isinstance(obj,model_types) and id(obj) not in new_ids and session.is_modified(obj,include_collections=True):
                    raise ImportSafetyError('Import attempted to modify an existing asset')
        session=self.db.session()
        event.listen(session,'before_flush',before_flush)
        previous_disable=logging.root.manager.disable
        try:
            # Import exceptions can include connection details; emit a safe
            # error type in main, not a Superset exception containing a URI.
            logging.disable(logging.CRITICAL)
            ImportDashboardsCommand(contents,overwrite=False,passwords=passwords).run()
        except Exception:
            session.rollback()
            raise
        finally:
            logging.disable(previous_disable)
            event.remove(session,'before_flush',before_flush)


def execute_import(*,bundle_path,bundle_version,username,trino_uri,backend,check_only=False,on_conflict='fail'):
    if bundle_version!=EXPECTED_SUPERSET:
        raise ImportSafetyError('Declare a bundle exported by the matching Superset 4.1.2')
    contents,ids,db_path=read_bundle(bundle_path)
    contents,passwords,expected_endpoint=prepare_contents(contents,ids,db_path,trino_uri)
    conflicts={kind:[] for kind in ('datasets','charts','dashboards')}
    for kind in conflicts:
        for identifier in ids[kind]:
            if backend.find_uuid(kind,identifier) is not None:conflicts[kind].append(identifier)
    db_uuid,(db_rel,db_config)=next(iter(ids['databases'].items()))
    existing_db=backend.find_uuid('databases',db_uuid)
    same_name=backend.find_database_name(db_config.get('database_name'))
    if existing_db is not None:
        if endpoint_identity(existing_db.sqlalchemy_uri)!=expected_endpoint:
            raise ImportSafetyError('Existing database UUID points to a different Trino endpoint; configure it separately')
        if same_name is not None and str(same_name.uuid)!=db_uuid:
            raise ImportSafetyError('Database name is already used by another UUID')
        # Preserve the existing Superset database and its stored credentials.
        passwords.pop(db_path,None)
    elif same_name is not None:
        raise ImportSafetyError('Database name is already used by another UUID')
    conflict_count=sum(map(len,conflicts.values()))
    result={'asset_counts':{k:len(v) for k,v in ids.items()},'conflicting_uuids':conflicts,
        'existing_database_reused':existing_db is not None,'status':'ready'}
    if conflict_count:
        if on_conflict=='skip':result['status']='skipped_entire_bundle_conflict';return result
        raise ImportSafetyError(f'Existing dataset/chart/dashboard UUIDs: {conflict_count}; no assets imported')
    if check_only:result['status']='validated_only';return result
    backend.import_bundle(contents,passwords)
    result['status']='imported'
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',default=os.getenv('SUPERSET_ASSET_BUNDLE','/app/docker/superset/assets/dashboards.zip'))
    parser.add_argument('--bundle-version',default=os.getenv('SUPERSET_ASSET_SOURCE_VERSION'),help='Declared source Superset version, e.g. 4.1.2')
    parser.add_argument('--username',default=os.getenv('SUPERSET_IMPORT_USERNAME','admin'))
    parser.add_argument('--check-only',action='store_true')
    parser.add_argument('--auto',action='store_true',help='Run only if SUPERSET_AUTO_IMPORT_DASHBOARDS=true')
    parser.add_argument('--on-conflict',choices=['fail','skip'])
    args=parser.parse_args(argv)
    if args.auto and os.getenv('SUPERSET_AUTO_IMPORT_DASHBOARDS','false').strip().lower()!='true':
        print(json.dumps({'status':'auto_import_disabled'}));return 0
    try:
        installed=importlib.metadata.version('apache-superset')
        if installed!=EXPECTED_SUPERSET:raise ImportSafetyError('Target Superset must be exactly 4.1.2')
        uri=os.environ.get('SUPERSET_TRINO_SQLALCHEMY_URI')
        if not uri:raise ImportSafetyError('Set SUPERSET_TRINO_SQLALCHEMY_URI in the container environment')
        from superset import create_app
        app=create_app()
        with app.app_context():
            result=execute_import(bundle_path=args.bundle,bundle_version=args.bundle_version,username=args.username,
                trino_uri=uri,backend=SupersetBackend(args.username),check_only=args.check_only,
                on_conflict=args.on_conflict or ('skip' if args.auto else 'fail'))
        print(json.dumps(result,ensure_ascii=False));return 0
    except ImportSafetyError as exc:
        print(f'Import refused: {exc}',file=sys.stderr);return 1
    except Exception as exc:
        # Do not print exception content/traceback: it may contain credentials.
        print(f'Import failed ({type(exc).__name__}); inspect Superset logs with credential protection.',file=sys.stderr);return 1


if __name__=='__main__':sys.exit(main())
