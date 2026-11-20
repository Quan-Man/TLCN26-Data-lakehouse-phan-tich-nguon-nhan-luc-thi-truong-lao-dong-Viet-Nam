"""Normalize the 18 raw Delta tables; no fabricated values or provenance."""
import os, re, csv, hashlib, unicodedata
from pyspark.sql import SparkSession, functions as F, types as T
from decimal import Decimal, InvalidOperation, localcontext
from datetime import datetime, timezone
from pathlib import Path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
TABLES = {1:'gdp',2:'gdp_growth',3:'new_enterprises',4:'investment',5:'labour_force',
 6:'employment',7:'unemployment',8:'income',9:'iip',10:'cpi',11:'fdi',
 12:'retail_sales',13:'enterprise_count',14:'underemployment',15:'occupation',
 16:'labour_structure',17:'training',18:'productivity'}
YEAR_COLUMN = re.compile(r'^(?P<measure>.+)_(?P<year>20\d{2})$')
META_COLUMNS = {'year','unit','notes','location','industry','occupation','area_type',
 'region','indicator','project_count_unit','capital_unit','employment_unit','share_unit','geography_version'}
FIXED_MEASURES = {1:{'gdp_total_value','agriculture_value','industry_construction_value',
 'service_value','product_tax_value','gdp_total_share','agriculture_share',
 'industry_construction_share','service_share','product_tax_share'},
 11:{'project_count','registered_capital_usd_million'}}
RAW_METADATA = {'raw_id','source_file','source_row_number','source_record_hash',
 'data_group','ingestion_batch_id','loaded_at'}


BRONZE='s3a://bronze/economic'; SILVER='s3a://silver/economic'
GROUPS=('economic','labour','enterprise')
GROUP={1:'economic',2:'economic',3:'enterprise',4:'enterprise',5:'labour',6:'labour',7:'labour',8:'labour',9:'economic',10:'economic',11:'enterprise',12:'economic',13:'enterprise',14:'labour',15:'labour',16:'labour',17:'labour',18:'economic'}
RULES={2:('gdp_index','Chỉ số phát triển GRDP','INDEX','NONE'),3:('new_enterprises','Doanh nghiệp mới','COUNT','SUM'),4:('investment','Vốn đầu tư','AMOUNT','SUM'),5:('labour_force','Lực lượng lao động','COUNT','SUM'),6:('employment','Lao động có việc làm','COUNT','SUM'),7:('unemployment_rate','Tỷ lệ thất nghiệp','RATE','NONE'),8:('average_monthly_income','Thu nhập bình quân','AVERAGE','NONE'),9:('iip','Chỉ số sản xuất công nghiệp','INDEX','NONE'),10:('cpi','CPI','INDEX','NONE'),12:('retail_sales','Doanh thu bán lẻ','AMOUNT','SUM'),13:('enterprise_count','Doanh nghiệp hoạt động','COUNT','SUM'),14:('rate','Tỷ lệ thiếu việc làm','RATE','NONE'),15:('employment','Lao động theo nghề','COUNT','SUM'),17:('rate','Tỷ lệ lao động qua đào tạo','RATE','NONE'),18:('productivity','Năng suất lao động','AVERAGE','NONE')}
def make_spark():
    user = os.getenv('MINIO_ROOT_USER') or os.environ['AWS_ACCESS_KEY_ID']
    password = os.getenv('MINIO_ROOT_PASSWORD') or os.environ['AWS_SECRET_ACCESS_KEY']
    return (SparkSession.builder.appName('silver-economic-labour').master('local[2]')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.hadoop.fs.s3a.endpoint', os.getenv('MINIO_ENDPOINT', 'http://minio:9000'))
            .config('spark.hadoop.fs.s3a.access.key', user)
            .config('spark.hadoop.fs.s3a.secret.key', password)
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(os.getenv('MINIO_ENDPOINT','http://minio:9000').startswith('https://')).lower())
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.sql.shuffle.partitions', '8')
            .config('spark.sql.ansi.enabled', 'false')
            .getOrCreate())


def bounded_rows(df, limit=100000):
    rows=df.limit(limit+1).collect()
    if len(rows)>limit: raise ValueError('Bronze safety limit exceeded')
    return [r.asDict(recursive=True) for r in rows]

def norm(x):
    return ' '.join(''.join(c for c in unicodedata.normalize('NFD',str(x or '').lower().replace('đ','d').replace('ð','d')) if not unicodedata.combining(c)).split())
def key(*x): return hashlib.sha256('|'.join(map(str,x)).encode()).hexdigest()
def category(kind,name):
    n=norm(name)
    if n in ('tong so','tong cong','all'): return 'ALL'
    # Normalize spelling and punctuation only, never merge distinct aggregate categories.
    n=re.sub(r'[^a-z0-9]+',' ',n).strip()
    if kind=='IND' and n.startswith('hoat dong lam thue cac cong viec trong cac ho gia dinh'): n='hoat dong lam thue ho gia dinh'
    return kind+'_'+key(n)[:24]
def gdp_industry_id(code):
    sector = code.rsplit('_', 1)[0]
    if sector == 'gdp_total': return 'ALL'
    if sector == 'agriculture':
        return category('IND', 'Nông nghiệp, lâm nghiệp và thủy sản')
    if sector in ('industry_construction', 'service', 'product_tax'):
        return 'NOT_APPLICABLE'
    raise ValueError(f'Unexpected GDP indicator: {code}')

def csvrows(path):
    with open(path,encoding='utf-8-sig',newline='') as f: return list(csv.DictReader(f))
def locations(root):
    result={}; ids=set()
    def add(label,id):
        if label: result.setdefault(norm(label),set()).add(id)
    for r in csvrows(Path(root)/'location_dictionary.csv'):
        id=r['location_id'];
        if not id.startswith('LOC_VN63_'): continue
        ids.add(id); add(r.get('province_name') or r.get('location_name'),id); add(id,id)
    for r in csvrows(Path(root)/'location_aliases.csv'):
        if r['location_id'] in ids and str(r.get('requires_context','')).lower() not in ('1','true','yes'): add(r['alias'],r['location_id'])
    for x in ('TP.Hồ Chí Minh','TP. Hồ Chí Minh','Thành phố Hồ Chí Minh'): add(x,'LOC_VN63_79')
    for x in ('Cả nước','Việt Nam','Toàn quốc'): add(x,'COUNTRY_VN')
    if len(ids)!=63: raise ValueError(f'Expected 63 province IDs, got {len(ids)}')
    return result
def location_id(label,lookup):
    ids=lookup.get(norm(label),set())
    if len(ids)!=1: raise ValueError(f'Unmapped/ambiguous location: {label}: {ids}')
    return next(iter(ids))
def parse_value(raw):
    if norm(raw) in ('','null','none','nan','n/a','-','--','chua co','chua cong bo','khong ro'): return None
    x=re.sub(r'\s+','',raw)
    if re.fullmatch(r'[-+]?\d{1,3}(,\d{3})+(\.\d+)?',x): x=x.replace(',','')
    elif re.fullmatch(r'[-+]?\d+,\d{1,2}',x): x=x.replace(',','.')
    elif not re.fullmatch(r'[-+]?\d+(\.\d+)?',x): raise ValueError(f'Invalid number: {raw}')
    with localcontext() as ctx:
        ctx.prec=40
        v=Decimal(x)
        if abs(v)>=Decimal(10)**18 or v!=v.quantize(Decimal('0.000001')): raise ValueError(f'Value exceeds Decimal(24,6): {raw}')
        return v

def normalize_row(n,r,lookup,loaded):
    if r.get('data_group') != GROUP[n]: raise ValueError(f'Unexpected Bronze group: {n}, {r.get("data_group")}')
    if n==14 and r.get('area_type') and norm(r['area_type']) not in ('ca nuoc','tong so','toan quoc'): raise ValueError('Residence breakdown cannot be represented in new schema')
    group=GROUP[n]; loc=location_id((r.get('location') or r.get('region')),lookup) if n in (2,3,5,6,7,8,9,12,13,14,17) else 'COUNTRY_VN'
    ind=category('IND',r['industry']) if n in (4,16,18) else ('ALL' if n in (2,3,5,6,7,8,11,12,13,14,15,17) else 'NOT_APPLICABLE')
    occ=category('OCC',r['occupation']) if n==15 else ('ALL' if group=='labour' else 'NOT_APPLICABLE')
    measures=[]
    if n==1:
        for sector,label in [('gdp_total','Tổng GDP'),('agriculture','Nông nghiệp, lâm nghiệp và thủy sản'),('industry_construction','Công nghiệp và xây dựng'),('service','Dịch vụ'),('product_tax','Thuế sản phẩm')]:
            for suffix,unit,typ,agg in [('value','Tỷ đồng','AMOUNT','SUM'),('share','%','SHARE','NONE')]:
                col=sector+'_'+suffix; measures.append((int(r['year']),col,col,label+(' – giá trị' if suffix=='value' else ' – tỷ trọng'),unit,typ,agg))
    elif n==11:
        for col,label,unit in [('project_count','Số dự án FDI','project_count_unit'),('registered_capital_usd_million','Vốn FDI đăng ký','capital_unit')]: measures.append((int(r['year']),col,col,label,r[unit],'COUNT' if col=='project_count' else 'AMOUNT','SUM'))
    else:
        rules=[RULES[n]] if n!=16 else [('employment','Lao động theo ngành','COUNT','SUM'),('share','Tỷ trọng lao động theo ngành','SHARE','NONE')]
        for prefix,label,typ,agg in rules:
            code={14:'underemployment_rate',17:'trained_labour_rate',15:'employment_by_occupation'}.get(n,prefix)
            if n==16: code={'employment':'employment_by_industry','share':'employment_share_by_industry'}[prefix]
            if n==10: code='cpi_'+key(norm(r['indicator']))[:20]; label=r['indicator'].strip()
            for year in range(2015,2026):
                col=f'{prefix}_{year}'; unit=r['share_unit' if prefix=='share' else 'employment_unit'] if n==16 else r['unit']; measures.append((year,col,code,label,unit,typ,agg))
    actual={c for c in r if c not in RAW_METADATA and c not in META_COLUMNS}
    if actual!={m[1] for m in measures}: raise ValueError(f'Unexpected/missing columns: {n}: {actual ^ {m[1] for m in measures}}')
    for year,col,code,label,unit,typ,agg in measures:
        if not unit or not str(unit).strip(): raise ValueError(f'Missing unit: {n}/{col}')
        if not 2015<=year<=2025: raise ValueError(f'Unexpected year {year}')
        raw=str(r[col] or '').strip(); val=parse_value(raw)
        if val is not None and not val.is_finite(): raise ValueError(raw)
        measure_ind = gdp_industry_id(code) if n == 1 else ind
        status='MISSING' if val is None else 'ACTUAL'
        notes=r.get('notes') or ''
        if n==10: notes+=' | Phạm vi địa lý: Cả nước (theo xác nhận của người cung cấp dữ liệu).'
        item={group+'_statistic_id':key(r['raw_id'],col),'year':year,'location_id':loc,'industry_id':measure_ind,'industry_scope':measure_ind if measure_ind in ('ALL','NOT_APPLICABLE','UNKNOWN') else 'SPECIFIC','indicator_code':code,'indicator_name':label,'indicator_value':val,'indicator_type':typ,'unit':unit,'aggregation_method':agg,'value_status':status,'notes':notes,'source_file':r['source_file'],'source_raw_id':r['raw_id'],'loaded_at':r['loaded_at']}
        if group=='labour': item.update(occupation_id=occ,occupation_scope=occ if occ in ('ALL','NOT_APPLICABLE','UNKNOWN') else 'SPECIFIC')
        yield item

def schema(group):
    from pyspark.sql import types as T
    fields=[(group+'_statistic_id',T.StringType()),('year',T.IntegerType()),('location_id',T.StringType()),('industry_id',T.StringType()),('industry_scope',T.StringType())]
    if group=='labour': fields += [(x,T.StringType()) for x in ('occupation_id','occupation_scope')]
    fields += [(x,T.StringType()) for x in ('indicator_code','indicator_name')]+[('indicator_value',T.DecimalType(24,6))]+[(x,T.StringType()) for x in ('indicator_type','unit','aggregation_method','value_status','notes','source_file','source_raw_id')]+[('loaded_at',T.TimestampType())]
    return T.StructType([T.StructField(k,t,True) for k,t in fields])
def prepare(spark,input_path=BRONZE):
    lookup=locations(os.getenv('SHARED_MAPPING_ROOT','/opt/airflow/configs/mappings'))
    rows={g:[] for g in GROUPS}; seen=set(); grain=set()
    for n,table in TABLES.items():
        df=spark.read.format('delta').load(f'{input_path}/{table}')
        if RAW_METADATA-set(df.columns): raise ValueError(f'Missing Bronze metadata: {table}')
        raw=bounded_rows(df)
        if not raw: raise ValueError(f'Empty Bronze table: {table}')
        for r in raw:
            if not r['raw_id'] or r['raw_id'] in seen: raise ValueError('Duplicate/null raw_id')
            seen.add(r['raw_id'])
            for item in normalize_row(n,r,lookup,None):
                k=tuple(item.get(c) for c in ('year','location_id','industry_id','occupation_id','indicator_code'))
                if k in grain: raise ValueError(f'Duplicate business grain: {k}')
                grain.add(k); rows[GROUP[n]].append(item)
        print(f'BRONZE OK: {table}: {len(raw)} rows',flush=True)
    return rows

def run_silver(input_path=BRONZE,output_root=SILVER,dry_run=False):
    if input_path.rstrip('/')!=BRONZE or output_root.rstrip('/')!=SILVER: raise ValueError('Only official roots allowed')
    spark=make_spark()
    try:
        rows=prepare(spark,input_path)
        for g,rs in rows.items():
            print(f'{g}_statistics: {len(rs)} observations, {sum(r["indicator_value"] is None for r in rs)} missing')
        if not dry_run:
            for g,rs in rows.items():
                target=f'{SILVER}/{g}_statistics'
                spark.createDataFrame(rs,schema(g)).write.format('delta').mode('overwrite').option('overwriteSchema','true').save(target)
                if spark.read.format('delta').load(target).count()!=len(rs): raise ValueError(f'Count mismatch {target}')
        return {g:len(rs) for g,rs in rows.items()}
    finally: spark.stop()
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(); p.add_argument('--dry-run',action='store_true'); print(run_silver(**vars(p.parse_args())))