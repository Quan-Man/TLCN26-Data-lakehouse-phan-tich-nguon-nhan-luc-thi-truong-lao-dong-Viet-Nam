"""Publication contracts, immutable receipts and JSON control artifacts.

No Spark, boto3, Trino or Airflow imports/IO on module import. A receipt is
written only AFTER source Gold DQ passes. Original Delta versions must remain
readable until the publisher snapshots them; never mark a task successful on
receipt write failure. Object keys are immutable; use a new batch ID to revise.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse

SOURCES = ('reference', 'education', 'jobs', 'economy')
RESERVED = {'mart_publication_sources', 'agg_job_posting_monthly', 'agg_job_skill_monthly', 'publication_manifest'}


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def token(value, label='identifier'):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',value) or '..' in value:
        raise ValueError(f'Invalid {label}')
    return value


def sql_name(value):
    if not isinstance(value,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,100}',value):
        raise ValueError('Invalid SQL identifier')
    return '"'+value+'"'


def sql_literal(value):
    return "'"+str(value).replace("'","''")+"'"


def iso_day(value):
    if not isinstance(value,str) or date.fromisoformat(value).isoformat()!=value:
        raise ValueError('Expected ISO as_of date')
    return value


class JsonStore:
    """Local paths for tests, s3[a]:// for MinIO. No mutable latest JSON pointer."""
    def __init__(self,root):
        self.root=root.rstrip('/')
        parsed=urlparse(root)
        if parsed.scheme in ('s3','s3a'):
            import boto3
            from botocore.config import Config
            access=os.getenv('MINIO_ROOT_USER') or os.getenv('AWS_ACCESS_KEY_ID')
            secret=os.getenv('MINIO_ROOT_PASSWORD') or os.getenv('AWS_SECRET_ACCESS_KEY')
            if not access or not secret:raise ValueError('Missing object storage credentials')
            self.client=boto3.client('s3',endpoint_url=os.getenv('MINIO_ENDPOINT','http://minio:9000'),
                aws_access_key_id=access,aws_secret_access_key=secret,
                region_name=os.getenv('AWS_DEFAULT_REGION','us-east-1'),
                config=Config(signature_version='s3v4',s3={'addressing_style':'path'},
                    connect_timeout=10,read_timeout=30,retries={'max_attempts':3}))
            self.bucket,self.prefix=parsed.netloc,parsed.path.strip('/')
        elif not parsed.scheme:
            self.client=None;self.local=Path(root).resolve()
        else:raise ValueError('Control root must be s3a://, s3:// or a local test directory')

    def _key(self,key):
        if key.startswith('/') or '..' in key.split('/'):raise ValueError('Invalid control key')
        return '/'.join(p for p in (getattr(self,'prefix',''),key) if p)

    def get(self,key):
        key=self._key(key)
        if self.client is None:
            path=self.local/key
            if not path.exists():return None
            data=path.read_bytes()
        else:
            from botocore.exceptions import ClientError
            try:response=self.client.get_object(Bucket=self.bucket,Key=key)
            except ClientError as exc:
                if exc.response['Error']['Code'] in ('NoSuchKey','404'):return None
                raise
            with response['Body'] as body:data=body.read(2*1024*1024+1)
        if len(data)>2*1024*1024:raise ValueError('Control artifact exceeds 2 MiB')
        return json.loads(data)

    def put_once(self,key,value):
        existing=self.get(key)
        if existing is not None:
            if existing!=value:raise ValueError(f'Immutable control artifact conflict: {key}')
            return key
        data=encode(value)
        if len(data)>2*1024*1024:raise ValueError('Control artifact exceeds 2 MiB')
        canonical=self._key(key)
        if self.client is None:
            # Write a private complete file, then publish via an exclusive hard
            # link. A reader never observes a half-written JSON file.
            import tempfile
            path=self.local/canonical;path.parent.mkdir(parents=True,exist_ok=True)
            fd,tmp=tempfile.mkstemp(dir=path.parent)
            try:
                with os.fdopen(fd,'wb') as handle:handle.write(data);handle.flush();os.fsync(handle.fileno())
                try:os.link(tmp,path)
                except FileExistsError:
                    if self.get(key)!=value:raise ValueError('Concurrent immutable artifact conflict')
            finally:os.unlink(tmp)
        else:
            from botocore.exceptions import ClientError
            try:self.client.put_object(Bucket=self.bucket,Key=canonical,Body=data,ContentType='application/json',IfNoneMatch='*')
            except ClientError as exc:
                if exc.response['Error']['Code'] not in ('PreconditionFailed','412','ConditionalRequestConflict','409'):raise
                if self.get(key)!=value:raise ValueError('Concurrent immutable artifact conflict') from exc
        return key


def load_config(path):
    config=json.loads(Path(path).read_text(encoding='utf-8'))
    required=config['required_sources']
    if not required or len(set(required))!=len(required) or not set(required).issubset(SOURCES):
        raise ValueError('Invalid required_sources')
    if 'reference' not in required:raise ValueError('Shared reference is required')
    tables=config['tables']
    if not tables or any(name in RESERVED for name in tables):raise ValueError('Invalid/reserved publication table')
    for name,spec in tables.items():
        sql_name(name)
        if spec['source'] not in required or not spec['pk'] or len(set(spec['pk']))!=len(spec['pk']):
            raise ValueError(f'Invalid table contract: {name}')
        if not isinstance(spec['path'],str) or spec['path'].startswith('s3://'):raise ValueError('Spark paths use s3a://')
    for source in required:
        if not any(t['source']==source for t in tables.values()):raise ValueError('Every required source needs table contracts')
    for name in ('catalog','storage_schema','analytics_schema'):sql_name(config['trino'][name])
    if config['trino']['storage_schema']==config['trino']['analytics_schema']:raise ValueError('Separate storage and BI schemas')
    token(config['channel'],'channel')
    if config['published_root'].rstrip('/')==config['control_root'].rstrip('/'):
        raise ValueError('Separate published and JSON control roots')
    return config


def make_plan(config, *, batch_id,period,as_of,source_periods=None,carry_forward=None):
    token(batch_id,'batch_id');token(period,'period');iso_day(as_of)
    source_periods=source_periods or {};carry_forward=carry_forward or {}
    if not set(source_periods).issubset(config['required_sources']) or not set(carry_forward).issubset(config['required_sources']):
        raise ValueError('Unconfigured source in period/carry policy')
    if 'reference' in carry_forward:raise ValueError('Provide a current reference receipt; do not carry reference implicitly')
    for source,value in source_periods.items():token(value,'source period')
    if any(not isinstance(reason,str) or not reason.strip() for reason in carry_forward.values()):
        raise ValueError('Carry-forward requires a nonempty reason for each source')
    result={'config':config,'batch_id':batch_id,'period':period,'as_of':as_of,
        'source_periods':source_periods,'carry_forward':carry_forward}
    result['plan_id']=digest(result)
    return result


def receipt_key(batch,source):
    token(batch,'batch_id');token(source,'source')
    return f'receipts/{batch}/{source}.json'


def reference_id(tables):
    return digest({name:{'path':t['path'].rstrip('/'),'version':t['version']} for name,t in tables.items()})


def validate_receipt(receipt,source,config):
    if receipt.get('status')!='READY' or receipt.get('source')!=source:raise ValueError(f'{source}: receipt not READY')
    token(receipt['batch_id'],'receipt batch');token(receipt['period'],'receipt period');iso_day(receipt['as_of'])
    dq=receipt.get('dq',{})
    if dq.get('status')!='PASSED' or not dq.get('checks') or any(c.get('passed') is not True or not c.get('name') for c in dq['checks']):
        raise ValueError(f'{source}: Gold DQ has not passed')
    expected={n:t for n,t in config['tables'].items() if t['source']==source}
    if set(receipt['tables'])!=set(expected):raise ValueError(f'{source}: incomplete/unexpected table set')
    for name,contract in expected.items():
        table=receipt['tables'][name]
        if table['path'].rstrip('/')!=contract['path'].rstrip('/') or table['pk']!=contract['pk']:
            raise ValueError(f'{source}: table path/PK differs from configuration')
        if type(table['version']) is not int or table['version']<0 or type(table['rows']) is not int or table['rows']<0:
            raise ValueError('Invalid Delta version/row count')
        if not isinstance(table.get('schema_hash'),str) or not re.fullmatch('[0-9a-f]{64}',table['schema_hash']):
            raise ValueError('Missing schema fingerprint')
    if not isinstance(receipt.get('reference_id'),str) or not re.fullmatch('[0-9a-f]{64}',receipt['reference_id']):
        raise ValueError('Missing reference version identity')
    if source=='reference' and receipt['reference_id']!=reference_id(receipt['tables']):
        raise ValueError('Reference receipt identity does not match its table versions')
    return receipt


def sources_ready(plan,store):
    """Missing evidence waits; failed/mismatched evidence fails immediately."""
    ready=True
    for source in plan['config']['required_sources']:
        if source in plan['carry_forward']:continue
        receipt=store.get(receipt_key(plan['batch_id'],source))
        if receipt is None:ready=False;continue
        validate_receipt(receipt,source,plan['config'])
        expected_period=plan['source_periods'].get(source,plan['period'])
        if receipt['batch_id']!=plan['batch_id'] or receipt['period']!=expected_period or receipt['as_of']!=plan['as_of']:
            raise ValueError(f'{source}: receipt batch/period/as_of mismatch')
    return ready


def select_sources(plan,store,previous=None):
    selected={}
    for source in plan['config']['required_sources']:
        reason=plan['carry_forward'].get(source)
        if reason:
            if previous is None or source not in previous['sources']:raise ValueError('Carry-forward requires a previously published source')
            receipt=previous['sources'][source]['receipt']
            state='CARRIED_FORWARD'
        else:
            receipt=store.get(receipt_key(plan['batch_id'],source));state='FRESH'
            if receipt is None:raise ValueError('Missing required receipt')
            if receipt['batch_id']!=plan['batch_id'] or receipt['period']!=plan['source_periods'].get(source,plan['period']) or receipt['as_of']!=plan['as_of']:
                raise ValueError(f'{source}: wrong batch/period/as_of')
        validate_receipt(receipt,source,plan['config'])
        selected[source]={'state':state,'reason':reason,'period_aligned':receipt['period']==plan['period'],'receipt':receipt}
    if len({item['receipt']['reference_id'] for item in selected.values()})!=1:
        raise ValueError('Sources used different shared reference versions')
    return selected


def schema_hash(frame):
    # Field order/types are contractual; nullability may be relaxed on Delta IO.
    return digest([(f.name,f.dataType.jsonValue()) for f in frame.schema])


def validate_frame(frame,pk):
    from pyspark.sql import functions as F
    from functools import reduce
    from operator import or_
    if not set(pk).issubset(frame.columns):raise ValueError('Missing PK columns')
    if any(c.startswith('_publication_') for c in frame.columns):raise ValueError('Reserved publication metadata column')
    if frame.where(reduce(or_,[F.col(c).isNull() for c in pk])).limit(1).count():raise ValueError('NULL PK')
    if frame.groupBy(*pk).count().where('count > 1').limit(1).count():raise ValueError('Duplicate PK')
    return frame.count()


def seal_source_receipt(*,spark,store,config,source,batch_id,period,as_of,versions,dq,reference_identity=None):
    """Source DAG calls this AFTER Gold DQ; versions are explicit, never latest."""
    tables={}
    contracts={n:t for n,t in config['tables'].items() if t['source']==source}
    if set(versions)!=set(contracts):raise ValueError('Explicit versions required for every source table')
    for name,contract in contracts.items():
        version=versions[name]
        if type(version) is not int or version<0:raise ValueError('Invalid Delta version')
        frame=spark.read.format('delta').option('versionAsOf',version).load(contract['path'])
        rows=validate_frame(frame,contract['pk'])
        tables[name]=dict(contract,version=version,rows=rows,schema_hash=schema_hash(frame))
        tables[name].pop('source',None)
    identity=reference_id(tables) if source=='reference' else reference_identity
    receipt={'status':'READY','source':source,'batch_id':batch_id,'period':period,'as_of':as_of,
             'reference_id':identity,'dq':dq,'tables':tables}
    validate_receipt(receipt,source,config)
    store.put_once(receipt_key(batch_id,source),receipt)
    return receipt
