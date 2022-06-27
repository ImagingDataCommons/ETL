from google.cloud import bigquery
import json
from datetime import datetime,date
from utils import getHist, read_clin_file
import pytz

DEFAULT_SUFFIX='clinical'
DEFAULT_DESCRIPTION='clinical data'
DEFAULT_DATASET ='idc_v10_clinical'
DEFAULT_PROJECT ='idc-dev-etl'
CURRENT_VERSION = 'idc_v10'
LAST_VERSION = 'idc_v10'
LAST_DATASET = 'idc_v10_clinical'
DESTINATION_FOLDER='./clin_'+CURRENT_VERSION+'/'
CPTAC_SRC='isb-cgc-bq.CPTAC.clinical_gdc_current'

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def create_table_meta_cptac_row(cptac):
  src_table_id = CPTAC_SRC
  client = bigquery.Client()
  src_table = client.get_table(src_table_id)
  table_last_modified = str(src_table.modified)
  table_size = str(src_table.num_bytes)

  hist={}
  table_id = DEFAULT_PROJECT + '.' + LAST_DATASET + '.table_metadata'
  getHist(hist, table_id)

  sumArr=[]
  #for coll in cptac:
  sumDic = {}
  suffix = DEFAULT_SUFFIX
  table_description = DEFAULT_DESCRIPTION
  collection_id = str(cptac)
  table_name = 'cptac_clinical'
  sumDic['collection_id'] = collection_id
  sumDic['table_name'] = table_name
  sumDic['source_info']=[]
  sumDic['source_info'].append({})
  sumDic['source_info'][0]['table_last_modified']=table_last_modified
  sumDic['source_info'][0]['table_size'] = table_size
  sumDic['source_info'][0]['srcs']=[src_table_id]

  sumDic['dataset']=''
  sumDic['project']=''

  if table_name in hist:
    for nkey in hist[table_name]:
      if (nkey not in sumDic) and not (nkey == 'source_info'):
        sumDic[nkey] = hist[table_name][nkey]
      old_table_modified=sumDic['source_info']['table_last_modified']
      old_table_size=sumDic['source_info']['table_size']
      if not (old_table_modified == table_last_modified):
        sumDic['idc_version_table_prior']=sumDic['idc_version_table_updated']
        sumDic['idc_version_table_updated'] = CURRENT_VERSION
        sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
  else:
    sumDic['idc_version_table_added'] = CURRENT_VERSION
    sumDic['table_added_datetime'] = str(datetime.now(pytz.utc))
    sumDic['idc_version_table_prior'] = CURRENT_VERSION
    sumDic['idc_version_table_updated'] = CURRENT_VERSION
    sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
    sumDic['number_batches'] = 0
  sumArr.append(sumDic)
  return sumArr


def create_column_meta_cptac_rows(cptac):
  src_table_id = DEFAULT_PROJECT + '.' + DEFAULT_DATASET + '.cptac_clinical'
  client = bigquery.Client()
  src_table = client.get_table(src_table_id)
  newArr=[]
  valSet={}
  fieldSet={}
  for field in src_table.schema:
    curRec={}
    nm = field.name
    valSet[nm]=[]
    fieldSet[nm]=True
    type = field.field_type
    curRec['collection_id']=str(cptac)
    if nm == "submitter_id":
      curRec['case_col']=True
    else:
      curRec['case_col']=False
    curRec['table_name']='cptac_clinical'
    curRec['table_description']='clinical_data'
    curRec['variable_name'] = nm
    curRec['variable_label'] = nm
    curRec['data_type']=type
    curRec['batch']=[0]
    newArr.append(curRec)

  query = "select * from `" + src_table_id + "`"
  ii=1
  job = client.query(query)
  for row in job.result():
    for nm in fieldSet:
      val=row[nm]
      if fieldSet[nm] and (not val in valSet[nm]):
        valSet[nm].append(val)
        if len(valSet[nm])>20:
          fieldSet[nm]=False
          valSet.pop(nm)

  for rec in newArr:
    if rec['variable_name'] in valSet and len(valSet[rec['variable_name']])>0:
      valSet[rec['variable_name']] = [str(x) for x in valSet[rec['variable_name']]]
      valSet[rec['variable_name']].sort()
      rec['values'] = [{"option_code":x} for x in valSet[rec['variable_name']]]
  return newArr

def copy_cptac():
  src_table_id = CPTAC_SRC 
  client = bigquery.Client()
  src_table = client.get_table(src_table_id)
  nschema=[bigquery.SchemaField("dicom_patient_id","STRING"),
          bigquery.SchemaField("source_batch","INTEGER")]
  nschema.extend(src_table.schema)

  dest_table_id = DEFAULT_PROJECT + '.' + DEFAULT_DATASET + '.cptac_clinical'
  client.delete_table(dest_table_id, not_found_ok=True)

  query = "select submitter_id as dicom_patient_id, 0 as source_batch, * from `" + src_table_id + "`"
  job_config=bigquery.QueryJobConfig(destination=dest_table_id)
  query_job=client.query(query, job_config=job_config)
  print(query_job.result())



def get_cptac():
  cptac=[]
  client = bigquery.Client()
  query = "select tcia_api_collection_id, tcia_wiki_collection_id, idc_webapp_collection_id from `idc-dev-etl.idc_current.original_collections_metadata` order by `tcia_wiki_collection_id`"
  job = client.query(query)
  cptac=[]

  for row in job.result():
    tcia_api=row['tcia_api_collection_id']
    wiki_collec=row['tcia_wiki_collection_id']
    idc_webapp=row['idc_webapp_collection_id']
    if idc_webapp.startswith('cptac_'):
      cptac.append(idc_webapp)
  cptac.sort()
  return cptac

if __name__=="__main__":
  cptac=get_cptac()  
  print(cptac)
  urow = create_table_meta_cptac_row(cptac)
  print(urow)
  copy_cptac()
  cm=create_column_meta_cptac_rows(cptac)
  print(len(cm))
