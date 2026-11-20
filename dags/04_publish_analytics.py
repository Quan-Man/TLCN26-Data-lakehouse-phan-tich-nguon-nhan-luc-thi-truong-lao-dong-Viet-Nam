from datetime import timedelta
from pathlib import Path
import os
import sys

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

DAG_ID='04_publish_analytics'
POOL='analytics_publication_writer'
PUBLISHED=Dataset('x-lakehouse://analytics/published')


def _modules():
    root=str(Path(__file__).resolve().parents[1])
    if root not in sys.path:sys.path.insert(0,root)
    from jobs.common import publication
    from jobs.gold import publish_analytics
    return publication,publish_analytics


def _plan(**context):
    p,_=_modules()
    path=os.getenv('PUBLICATION_CONFIG',str(Path(__file__).resolve().parents[1]/'configs/publication.json'))
    config=p.load_config(path);params=context['params']
    plan=p.make_plan(config,batch_id=params['batch_id'],period=params['period'],as_of=params['as_of'],
        source_periods=params['source_periods'],carry_forward=params['carry_forward'])
    key=f'plans/{plan["plan_id"]}.json';p.JsonStore(config['control_root']).put_once(key,plan)
    return {'control_root':config['control_root'],'plan_key':key}


def _load_plan(context):
    p,job=_modules();ref=context['ti'].xcom_pull(task_ids='plan_publication')
    if not ref:raise ValueError('Missing publication plan')
    store=p.JsonStore(ref['control_root']);plan=store.get(ref['plan_key'])
    if not plan:raise ValueError('Missing immutable plan artifact')
    return p,job,store,plan


def _wait(**context):
    from airflow.exceptions import AirflowFailException
    p,_,store,plan=_load_plan(context)
    try:return p.sources_ready(plan,store)
    except ValueError as exc:raise AirflowFailException(str(exc)) from exc


def _stage(**context):
    _,job,store,plan=_load_plan(context)
    return job.stage_publication(plan=plan,store=store)


def _candidate(context):
    _,job,store,_=_load_plan(context)
    ref=context['ti'].xcom_pull(task_ids='stage_analytics')
    if not ref:raise ValueError('Missing prepared candidate')
    candidate=store.get(ref['candidate_key'])
    if not candidate or candidate['publication_id']!=ref['publication_id']:raise ValueError('Candidate identity mismatch')
    return job,store,candidate


def _views(**context):
    job,store,candidate=_candidate(context)
    return {'view_check_key':job.prepare_views(candidate=candidate,store=store)}


def _activate(**context):
    job,store,candidate=_candidate(context)
    report=job.activate_publication(candidate=candidate,store=store)
    context['outlet_events'][PUBLISHED].extra=report
    return report


with DAG(dag_id=DAG_ID,start_date=pendulum.datetime(2026,9,1,tz='Asia/Ho_Chi_Minh'),
    schedule=None,catchup=False,max_active_runs=1,max_active_tasks=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={'owner':'lakehouse','retries':2,'retry_delay':timedelta(minutes=1),'depends_on_past':False},
    tags=['publication','analytics','batch','delta','trino'],doc_md=__doc__,
    params={'batch_id':Param(None,type=['null','string'],description='Required: shared orchestration batch ID'),
        'period':Param(None,type=['null','string'],description='Required: publication period, e.g. 2026-11'),
        'as_of':Param(None,type=['null','string'],description='Required ISO date; no system-date fallback'),
        'source_periods':Param({},type='object',additionalProperties={'type':'string'}),
        'carry_forward':Param({},type='object',additionalProperties={'type':'string'})}) as dag:
    ensure_pool=BashOperator(task_id='ensure_publication_pool',pool='default_pool',
        bash_command='airflow pools set analytics_publication_writer 1 "Single analytics publication writer"',
        execution_timeout=timedelta(minutes=2),do_xcom_push=False)
    plan=PythonOperator(task_id='plan_publication',python_callable=_plan,
        execution_timeout=timedelta(minutes=2),show_return_value_in_logs=False)
    wait=PythonSensor(task_id='wait_for_accepted_sources',python_callable=_wait,
        mode='reschedule',poke_interval=60,timeout=6*3600,retries=0,soft_fail=False,
        execution_timeout=timedelta(minutes=3))
    stage=PythonOperator(task_id='stage_analytics',python_callable=_stage,pool=POOL,pool_slots=1,
        execution_timeout=timedelta(minutes=45),show_return_value_in_logs=False)
    views=PythonOperator(task_id='prepare_and_verify_views',python_callable=_views,pool=POOL,pool_slots=1,
        execution_timeout=timedelta(minutes=20),show_return_value_in_logs=False)
    activate=PythonOperator(task_id='activate_publication',python_callable=_activate,pool=POOL,pool_slots=1,
        execution_timeout=timedelta(minutes=15),outlets=[PUBLISHED],show_return_value_in_logs=False)
    ensure_pool >> plan >> wait >> stage >> views >> activate
