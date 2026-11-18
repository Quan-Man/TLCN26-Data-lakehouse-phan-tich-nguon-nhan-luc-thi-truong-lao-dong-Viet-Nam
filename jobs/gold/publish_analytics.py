"""Stage verified version-pinned BI snapshots, create views, activate ONE pointer.

All published tables append immutable _publication_id partitions. BI views join
these partitions to a single Delta publication_manifest row. Failed staging
cannot make a partial release current. One Airflow publisher writer is required;
there is no claimed distributed transaction across the source tables/views.
"""
from __future__ import annotations

from datetime import datetime,timezone
import json
import os
import re

from jobs.common import publication as p
from jobs.gold import build_shared_dimensions as shared
from jobs.gold import build_aggregates as agg


def manifest_schema():
    from pyspark.sql import types as T
    return T.StructType([T.StructField(c,T.StringType()) for c in (
        'channel','publication_id','batch_id','period','as_of','status','source_states_json','manifest_json','published_at')])


def current_path(config):return config['published_root'].rstrip('/')+'/_control/publication_manifest'


def version(spark,path):
    from delta.tables import DeltaTable
    return int(DeltaTable.forPath(spark,path).history(1).first()['version'])


def current(spark,config):
    from pyspark.sql import functions as F
    path=current_path(config)
    frame=shared.existing_delta(spark,path,manifest_schema())
    if frame is None:return None,None
    rows=frame.where(F.col('channel')==config['channel']).limit(2).collect()
    if len(rows)>1:raise ValueError('Multiple current publications for one channel')
    return (rows[0].asDict() if rows else None),version(spark,path)


def same_rows(left,right,ignore=()):
    columns=[c for c in left.columns if c not in ignore]
    return not left.select(*columns).exceptAll(right.select(*columns)).limit(1).count() and not right.select(*columns).exceptAll(left.select(*columns)).limit(1).count()


def published_frame(frame,publication_id):
    from pyspark.sql import functions as F
    return frame.withColumn('_publication_id',F.lit(publication_id))


def preflight_target(spark,path,frame):
    if shared.hadoop_exists(spark,path):
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(spark,path):raise ValueError('Published path is not Delta')
        old=spark.read.format('delta').load(path)
        if p.schema_hash(old)!=p.schema_hash(frame):raise ValueError('Published schema changed; explicit BI migration required')


def append_snapshot(spark,path,frame,publication_id,ignore=()):
    from pyspark.sql import functions as F
    desired=published_frame(frame,publication_id)
    exists=shared.hadoop_exists(spark,path)
    if exists:
        old=spark.read.format('delta').load(path).where(F.col('_publication_id')==publication_id)
        if old.limit(1).count():
            if not same_rows(desired,old,ignore):raise ValueError('Immutable publication partition differs on retry')
            return version(spark,path)
        if not desired.limit(1).count():return version(spark,path)
    desired.write.format('delta').mode('append' if exists else 'errorifexists').partitionBy('_publication_id').save(path)
    return version(spark,path)


def source_mart(spark,sources):
    rows=[(source,entry['state'],entry['reason'],entry['receipt']['batch_id'],entry['receipt']['period'],
           entry['receipt']['as_of'],entry['period_aligned'],entry['receipt']['reference_id'],'PASSED')
          for source,entry in sorted(sources.items())]
    return spark.createDataFrame(rows,'source string, source_state string, carry_reason string, source_batch_id string, source_period string, source_as_of string, period_aligned boolean, reference_id string, dq_status string')


def stage_publication(*,plan,store,spark=None):
    config=plan['config'];owns=spark is None
    spark=spark or shared.make_spark('stage-publication',config['published_root'].startswith('s3a://'))
    cached=[]
    try:
        old,baseline_version=current(spark,config)
        previous=json.loads(old['manifest_json']) if old else None
        sources=p.select_sources(plan,store,previous)
        publication_id=p.digest({'plan_id':plan['plan_id'],'sources':sources})
        key=f'candidates/{publication_id}.json'
        existing=store.get(key)
        if existing:
            if existing['plan_id']!=plan['plan_id']:raise ValueError('Candidate identity conflict')
            if old and old['publication_id']==publication_id:return {'publication_id':publication_id,'candidate_key':key}
            if existing['expected_previous']!=(old['publication_id'] if old else None):raise ValueError('Candidate baseline advanced; use a new batch')
            return {'publication_id':publication_id,'candidate_key':key}
        frames={};details={}
        for source,entry in sources.items():
            for name,receipt in entry['receipt']['tables'].items():
                frame=spark.read.format('delta').option('versionAsOf',receipt['version']).load(receipt['path'])
                if p.schema_hash(frame)!=receipt['schema_hash'] or p.validate_frame(frame,receipt['pk'])!=receipt['rows']:
                    raise ValueError(f'{name}: source snapshot does not match accepted receipt')
                frames[name]=frame
                details[name]={'source':source,'origin':receipt,'pk':receipt['pk'],'rows':receipt['rows']}
        derived={}
        if 'jobs' in sources:
            dimensions={name:frames[f'dim_{name}'] for name in (*shared.SPECS,'date')}
            for name,spec in shared.SPECS.items():
                shared.validate_dimension(shared.bounded_rows(dimensions[name],100000),spec)
            parent,child=frames['fact_job_posting'],frames['fact_job_skill']
            as_of=sources['jobs']['receipt']['as_of']
            as_of,counts=agg.validate_inputs(parent,child,dimensions,as_of)
            population=agg.prepare_population(parent,counts,dimensions).cache();cached.append(population)
            skilled=child.select('job_fact_key','skill_id','is_required').join(population,'job_fact_key','inner').cache();cached.append(skilled)
            for kind,name,population_for_agg in [('posting',agg.TABLES[0],population),('skill',agg.TABLES[1],skilled)]:
                frame=agg.aggregate_population(population_for_agg,kind,as_of).cache();cached.append(frame)
                derived[name]=agg.reconcile(frame,population_for_agg,kind)
                frames[name]=frame
                details[name]={'source':'derived','pk':['aggregate_key'],'rows':frame.count(),'reconciliation':derived[name]}
        name='mart_publication_sources';frames[name]=source_mart(spark,sources)
        details[name]={'source':'derived','pk':['source'],'rows':len(sources)}
        # Preflight EVERY destination schema before any append or activation.
        for name,frame in frames.items():
            path=config['published_root'].rstrip('/')+'/tables/'+name
            preflight_target(spark,path,published_frame(frame,publication_id))
        if baseline_version is not None and version(spark,current_path(config))!=baseline_version:
            raise ValueError('Publication pointer changed during preparation')
        datasets={}
        for name,frame in frames.items():
            path=config['published_root'].rstrip('/')+'/tables/'+name
            output_version=append_snapshot(spark,path,frame,publication_id,ignore=('loaded_at',) if name in derived else ())
            datasets[name]=dict(details[name],path=path,version=output_version,schema_hash=p.schema_hash(published_frame(frame,publication_id)))
        if baseline_version is None:
            spark.createDataFrame([],manifest_schema()).write.format('delta').mode('errorifexists').save(current_path(config))
            baseline_version=version(spark,current_path(config))
        candidate={'publication_id':publication_id,'plan_id':plan['plan_id'],'batch_id':plan['batch_id'],
            'period':plan['period'],'as_of':plan['as_of'],'channel':config['channel'],
            'expected_previous':old['publication_id'] if old else None,'baseline_version':baseline_version,
            'status':'VALIDATED','sources':sources,'datasets':datasets,'config':config}
        store.put_once(key,candidate)
        return {'publication_id':publication_id,'candidate_key':key}
    finally:
        for frame in reversed(cached):frame.unpersist()
        if owns:spark.stop()


class TrinoSQL:
    def __init__(self,config):
        from trino.dbapi import connect
        from trino.auth import BasicAuthentication
        password=os.getenv('TRINO_PASSWORD')
        self.connection=connect(host=os.getenv('TRINO_HOST','trino'),port=int(os.getenv('TRINO_PORT','8080')),
            user=os.getenv('TRINO_USER','publisher'),catalog=config['trino']['catalog'],
            http_scheme=os.getenv('TRINO_HTTP_SCHEME','http'),request_timeout=60,max_attempts=3,
            auth=BasicAuthentication(os.getenv('TRINO_USER','publisher'),password) if password else None)
    def query(self,sql):
        cursor=self.connection.cursor()
        try:cursor.execute(sql);return cursor.fetchall()
        finally:cursor.close()
    def close(self):self.connection.close()


def qualified(config,schema,table):
    return '.'.join(map(p.sql_name,[config['trino']['catalog'],schema,table]))


def trino_location(path):
    if not path.startswith('s3a://'):raise ValueError('Trino registration requires production s3a:// paths')
    return 's3://'+path[6:]


def register_table(query,config,name,path):
    catalog=p.sql_name(config['trino']['catalog']);schema=config['trino']['storage_schema']
    exists=query(f'SELECT table_type FROM {catalog}.information_schema.tables WHERE table_schema={p.sql_literal(schema)} AND table_name={p.sql_literal(name)}')
    if not exists:
        query(f'CALL {catalog}.system.register_table(schema_name => {p.sql_literal(schema)}, table_name => {p.sql_literal(name)}, table_location => {p.sql_literal(trino_location(path))})')
    elif len(exists)!=1 or exists[0][0]!='BASE TABLE':raise ValueError('Existing registered object is not a base table')
    ddl=query('SHOW CREATE TABLE '+qualified(config,schema,name))[0][0]
    locations=re.findall(r"\blocation\s*=\s*'((?:''|[^'])*)'",ddl,re.IGNORECASE)
    if len(locations)!=1 or locations[0].replace("''", "'").rstrip('/')!=trino_location(path).rstrip('/'):
        raise ValueError('Registered Delta location differs; never reuse a wrong table')
    flush(query,config,name)


def flush(query,config,name):
    query(f'CALL {p.sql_name(config["trino"]["catalog"])}.system.flush_metadata_cache(schema_name => {p.sql_literal(config["trino"]["storage_schema"])}, table_name => {p.sql_literal(name)})')


def view_sql(config,name):
    storage=config['trino']['storage_schema'];bi=config['trino']['analytics_schema']
    return (f'CREATE OR REPLACE VIEW {qualified(config,bi,name)} SECURITY DEFINER AS '
        f'SELECT d.*, m.batch_id AS _publication_batch_id, m.period AS _publication_period, '
        f'm.source_states_json AS _publication_source_states FROM {qualified(config,storage,name)} d '
        f'JOIN {qualified(config,storage,"publication_manifest")} m ON d._publication_id=m.publication_id '
        f'WHERE m.channel={p.sql_literal(config["channel"])} AND m.status IN (\'PUBLISHED\',\'PUBLISHED_WITH_MIXED_PERIODS\')')


def prepare_views(*,candidate,store,query=None):
    config=candidate['config'];client=None
    if query is None:client=TrinoSQL(config);query=client.query
    try:
        for schema in (config['trino']['storage_schema'],config['trino']['analytics_schema']):
            query(f'CREATE SCHEMA IF NOT EXISTS {p.sql_name(config["trino"]["catalog"])}.{p.sql_name(schema)}')
        register_table(query,config,'publication_manifest',current_path(config))
        for name,table in candidate['datasets'].items():
            register_table(query,config,name,table['path'])
            count=query(f'SELECT count(*) FROM {qualified(config,config["trino"]["storage_schema"],name)} WHERE _publication_id={p.sql_literal(candidate["publication_id"])}')[0][0]
            if count!=table['rows']:raise ValueError(f'{name}: Trino cannot read the complete prepared snapshot')
            # View definitions are fixed across releases. Schema evolution was
            # rejected earlier; no view points to the pending release by itself.
            query(view_sql(config,name))
            query('SELECT * FROM '+qualified(config,config['trino']['analytics_schema'],name)+' WHERE false')
        approval={'publication_id':candidate['publication_id'],'candidate_hash':p.digest(candidate),'status':'PASSED'}
        key=f'view_checks/{candidate["publication_id"]}.json';store.put_once(key,approval)
        return key
    finally:
        if client:client.close()


def activate_publication(*,candidate,store,spark=None,query=None):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    config=candidate['config'];publication_id=candidate['publication_id']
    approval=store.get(f'view_checks/{publication_id}.json')
    if approval!={'publication_id':publication_id,'candidate_hash':p.digest(candidate),'status':'PASSED'}:
        raise ValueError('Trino view/snapshot checks have not passed for this candidate')
    owns=spark is None;spark=spark or shared.make_spark('activate-publication',config['published_root'].startswith('s3a://'))
    try:
        old,pointer_version=current(spark,config)
        if not old or old['publication_id']!=publication_id:
            if (old['publication_id'] if old else None)!=candidate['expected_previous'] or pointer_version!=candidate['baseline_version']:
                raise ValueError('Publication baseline changed; refuse stale activation/rollback')
            # Recheck the latest copy partitions against exactly the prepared
            # Delta versions. Other release partitions are irrelevant.
            for name,table in candidate['datasets'].items():
                expected=spark.read.format('delta').option('versionAsOf',table['version']).load(table['path']).where(F.col('_publication_id')==publication_id)
                latest=spark.read.format('delta').load(table['path']).where(F.col('_publication_id')==publication_id)
                if latest.count()!=table['rows'] or p.schema_hash(latest)!=table['schema_hash'] or not same_rows(expected,latest):
                    raise ValueError(f'{name}: prepared publication copy changed')
            mixed=any(s['state']=='CARRIED_FORWARD' or not s['period_aligned'] for s in candidate['sources'].values())
            status='PUBLISHED_WITH_MIXED_PERIODS' if mixed else 'PUBLISHED'
            row={'channel':config['channel'],'publication_id':publication_id,'batch_id':candidate['batch_id'],
                'period':candidate['period'],'as_of':candidate['as_of'],'status':status,
                'source_states_json':p.encode({s:{k:v for k,v in x.items() if k!='receipt'} | {
                    'batch_id':x['receipt']['batch_id'],'period':x['receipt']['period'],'as_of':x['receipt']['as_of']} for s,x in candidate['sources'].items()}).decode(),
                'manifest_json':p.encode(candidate).decode(),'published_at':datetime.now(timezone.utc).isoformat()}
            frame=spark.createDataFrame([row],manifest_schema())
            if version(spark,current_path(config))!=pointer_version:raise ValueError('Publication pointer changed before commit')
            expected_previous=p.sql_literal(candidate['expected_previous']) if candidate['expected_previous'] else 'NULL'
            (DeltaTable.forPath(spark,current_path(config)).alias('t').merge(frame.alias('s'),'t.channel = s.channel')
                .whenMatchedUpdateAll(condition=f't.publication_id <=> {expected_previous}').whenNotMatchedInsertAll().execute())
            actual,_=current(spark,config)
            if not actual or actual['publication_id']!=publication_id:raise ValueError('Publication compare-and-set failed')
        # A cache/network failure AFTER the pointer commit is retried here.
        # Never roll back a valid active publication automatically.
        client=None
        if query is None:client=TrinoSQL(config);query=client.query
        try:
            flush(query,config,'publication_manifest')
            active=query(f'SELECT publication_id FROM {qualified(config,config["trino"]["storage_schema"],"publication_manifest")} WHERE channel={p.sql_literal(config["channel"])}')
            if active!=[(publication_id,)] and active!=[[publication_id]]:raise ValueError('Trino publication pointer is not visible yet')
        finally:
            if client:client.close()
        row,_=current(spark,config)
        return {'publication_id':publication_id,'batch_id':candidate['batch_id'],'period':candidate['period'],
            'status':row['status'],'manifest_path':current_path(config),'manifest_version':version(spark,current_path(config))}
    finally:
        if owns:spark.stop()
