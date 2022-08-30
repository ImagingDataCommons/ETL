from google.cloud import bigquery
import json
from datetime import datetime,date
from utils import getHist, read_clin_file
import pytz

DEFAULT_SUFFIX="clinical"
DEFAULT_DESCRIPTION="clinical data"

CPTAC_SRC='isb-cgc-bq.CPTAC.clinical_gdc_current'
NLST='idc-dev-etl.idc_v11_pub'
NLST_SRCA=['nlst_canc','nlst_ctab','nlst_ctabc','nlst_prsn','nlst_screen']
TCGA_SRC='idc-dev-etl.idc_v11_pub.tcga_clinical_rel9'

IDC_COLLECTION_ID_SRC='`idc-dev-etl.idc_v11_pub.original_collections_metadata`'
IDC_PATIENT_ID_SRC='`idc-dev-etl.idc_v11_pub.dicom_all`'


SOURCE_BATCH_COL='source_batch'
SOURCE_BATCH_LABEL='idc_provenance_source_batch'
DICOM_COL= 'dicom_patient_id'
DICOM_LABEL='idc_provenance_dicom_patient_id'



def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def create_table_meta_row(collec,table_name,dataset_id,version,src_table_id):
  client = bigquery.Client()
  src_table = client.get_table(src_table_id)
  table_last_modified = str(src_table.modified)
  table_size = str(src_table.num_bytes)

  hist={}
  table_id = dataset_id + '.table_metadata'
  getHist(hist, table_id)

  sumArr=[]
  #for coll in cptac:
  sumDic = {}
  suffix = DEFAULT_SUFFIX
  table_description = DEFAULT_DESCRIPTION
  #collection_id = str(cptac)
  #table_name = 'cptac_clinical'
  sumDic['collection_id'] = collec
  sumDic['table_name'] = table_name
  sumDic['table_description'] = 'clinical_data'
  sumDic['source_info']=[]
  sumDic['source_info'].append({})
  sumDic['source_info'][0]['table_last_modified']=table_last_modified
  sumDic['source_info'][0]['table_size'] = table_size
  sumDic['source_info'][0]['srcs']=[src_table_id]

  if table_name in hist:
    for nkey in hist[table_name]:
      if (nkey not in sumDic) and not (nkey == 'source_info'):
        sumDic[nkey] = hist[table_name][nkey]
      old_table_modified=sumDic['source_info'][0]['table_last_modified']
      old_table_size=sumDic['source_info'][0]['table_size']
      if not (old_table_modified == table_last_modified):
        sumDic['idc_version_table_prior']=sumDic['idc_version_table_updated']
        sumDic['idc_version_table_updated'] = version
        sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
  else:
    sumDic['idc_version_table_added'] = version
    sumDic['table_added_datetime'] = str(datetime.now(pytz.utc))
    sumDic['idc_version_table_prior'] = version
    sumDic['idc_version_table_updated'] = version
    sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
    sumDic['number_batches'] = 0
  sumArr.append(sumDic)
  return sumArr


def create_column_meta_rows(collec, table_name,dataset_id):
  src_table_id = dataset_id + '.' + table_name
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
    curRec['collection_id']=collec
    if nm == "submitter_id":
      curRec['case_col']=True
    else:
      curRec['case_col']=False
    curRec['table_name']=table_name

    curRec['column'] = nm
    
    if nm == SOURCE_BATCH_COL:
      curRec['column_label']=SOURCE_BATCH_LABEL
    elif nm == DICOM_COL:
      curRec['column_label']=DICOM_LABEL
    else:
      curRec['column_label'] = nm
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
    if rec['column'] in valSet and len(valSet[rec['column']])>0:
      valSet[rec['column']] = [str(x) for x in valSet[rec['column']]]
      valSet[rec['column']].sort()
      rec['values'] = [{"option_code":x} for x in valSet[rec['column']]]
  return newArr

def copy_table(dataset_id, table_name, lst, src_table_id, id_col, intIds):
  if table_name is None:
    table_name="cptac_clinical"
  #src_table_id = CPTAC_SRC
  client = bigquery.Client()
  src_table = client.get_table(src_table_id)
  nschema=[bigquery.SchemaField("dicom_patient_id","STRING"),
          bigquery.SchemaField("source_batch","INTEGER")]
  nschema.extend(src_table.schema)

  dest_table_id = dataset_id + '.'+table_name
  client.delete_table(dest_table_id, not_found_ok=True)

  if lst is None:
    query = "select " + id_col + " as dicom_patient_id, 0 as source_batch, * from `" + src_table_id + "`"
  else:
    if intIds:
      qslst = [ str(x)  for x in lst]
      inp=",".join(qslst)
    else:
      qslst=["\"" + str(x) + "\"" for x in lst]
      inp =",".join(qslst)
    query = "select " + id_col + " as dicom_patient_id, 0 as source_batch, * from `" + src_table_id + "` where " + id_col + " in (" + inp + ")"
  job_config=bigquery.QueryJobConfig(destination=dest_table_id)
  query_job=client.query(query, job_config=job_config)
  print(query_job.result())
  dest_table=client.get_table(dest_table_id)
  nrows=dest_table.num_rows
  if nrows==0:
    client.delete_table(dest_table_id, not_found_ok=True)
  return(nrows)
  kk=1




def get_ids(program,collection):
  cptac=[]
  client = bigquery.Client()
  query = "select distinct t1.idc_webapp_collection_id, PatientID from "+IDC_COLLECTION_ID_SRC+" t1,"+IDC_PATIENT_ID_SRC+" t2 where "
  if (program is not None):
    query = query + "Program = '"+ program +"' and "
  if (collection is not None):
    query = query + "t1.idc_webapp_collection_id = '" + collection + "' and "
  query = query + "t1.idc_webapp_collection_id = t2.idc_webapp_collection_id "\
          "order by t1.idc_webapp_collection_id, PatientID"
  print(query)
  job = client.query(query)
  cptac=[]
  cptacDic={}
  cptacCol=set()
  for row in job.result():
    idc_webapp=row['idc_webapp_collection_id']
    patientID = row['PatientID']

    if not idc_webapp in cptacDic:
      cptacDic[idc_webapp]=[]
    cptacDic[idc_webapp].append(patientID)
  for collec in cptacDic:
    cptacDic[collec].sort()

  return cptacDic

def addTables(proj_id, dataset_id, version,program,collection,subscript,table_src, id_col,intIds):
  nrows=[]
  colrows=[]
  cptac = get_ids(program, collection)
  dataset_id = proj_id + "." + dataset_id
  for collec in cptac:
    table_name = collec + "_" + subscript
    numr = copy_table(dataset_id, table_name, cptac[collec],table_src, id_col, intIds)
    if numr > 0:
      nrows.extend(create_table_meta_row(collec, table_name, dataset_id, version,table_src))
      colrows.extend(create_column_meta_rows(collec, table_name, dataset_id))
  return([nrows,colrows])

if __name__=="__main__":
  ret=addTables("idc-dev","idc_v11_clinical","idc_v11","CPTAC",None,"clinical",CPTAC_SRC,"submitter_id", False)
  ret=addTables("idc-dev","idc_v11_clinical","idc_v11","TCGA",None,"clinical",TCGA_SRC,"case_barcode", False)
  for colec in NLST_SRCA:
    sufx=colec.split('_')[1].lower()
    src=NLST+'.'+colec
    nret = addTables("idc-dev", "idc_v11_clinical", "idc_v11", "NCI Trials", "nlst", sufx, src, "pid",True)
    rr=1
  rr=1


