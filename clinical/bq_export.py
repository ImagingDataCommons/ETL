from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys
from addcptac import addTables, CPTAC_SRC,TCGA_SRC

DEFAULT_SUFFIX='clinical'
DEFAULT_DESCRIPTION='clinical data'
DEFAULT_PROJECT ='idc-dev-etl'
DICOM_META='idc-dev-etl.idc_current.dicom_all'

#DEFAULT_PROJECT ='idc-dev'
CURRENT_VERSION = 'idc_v13'
LAST_VERSION = 'idc_v12'
FINAL_PROJECT='bigquery-public-data'

DATASET=CURRENT_VERSION+'_clinical'
LAST_DATASET=LAST_VERSION+'_clinical'


META_SUM_SCHEMA= [
          bigquery.SchemaField("collection_id","STRING"),
          bigquery.SchemaField("table_name","STRING"),
          bigquery.SchemaField("table_description", "STRING"),
          bigquery.SchemaField("idc_version_table_added", "STRING"),
          bigquery.SchemaField("table_added_datetime", "STRING"),
          bigquery.SchemaField("post_process_src","STRING"),
          bigquery.SchemaField("post_process_src_added_md5","STRING"),
          
          bigquery.SchemaField("idc_version_table_prior", "STRING"),
          bigquery.SchemaField("post_process_src_prior_md5", "STRING"),
          bigquery.SchemaField("idc_version_table_updated","STRING"),
          bigquery.SchemaField("table_updated_datetime","STRING"),
          bigquery.SchemaField("post_process_src_updated_md5","STRING"),
          
          bigquery.SchemaField("number_batches","INTEGER"),
          bigquery.SchemaField("source_info","RECORD",mode="REPEATED",
              fields=[bigquery.SchemaField("srcs","STRING",mode="REPEATED"),
              bigquery.SchemaField("md5","STRING"),
              bigquery.SchemaField("table_last_modified", "STRING"),
              bigquery.SchemaField("table_size", "INTEGER"),
            ]  
          ),

           ] 

def create_meta_summary(project, dataset, cptacColRows):
  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".table_metadata"
  #filenm=CURRENT_VERSION+"_table_metadata.json"
  schema = [
          bigquery.SchemaField("collection_id","STRING"),
          bigquery.SchemaField("table_name","STRING"),
          bigquery.SchemaField("table_description", "STRING"),
          bigquery.SchemaField("idc_version_table_added", "STRING"),
          bigquery.SchemaField("table_added_datetime", "STRING"),
          bigquery.SchemaField("post_process_src","STRING"),
          bigquery.SchemaField("post_process_src_added_md5","STRING"),
          
          bigquery.SchemaField("idc_version_table_prior", "STRING"),
          bigquery.SchemaField("post_process_src_prior_md5", "STRING"),
          bigquery.SchemaField("idc_version_table_updated","STRING"),
          bigquery.SchemaField("table_updated_datetime","STRING"),
          bigquery.SchemaField("post_process_src_updated_md5","STRING"),
          
          bigquery.SchemaField("number_batches","INTEGER"),
          bigquery.SchemaField("source_info","RECORD",mode="REPEATED",
              fields=[bigquery.SchemaField("srcs","STRING",mode="REPEATED"),
              bigquery.SchemaField("md5","STRING"),
              bigquery.SchemaField("table_last_modified", "STRING"),
              bigquery.SchemaField("table_size", "INTEGER"),
            ]  
          ),

           ] 


  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)

def load_meta_summary(project, dataset, cptacColRows,filenm):
  client = bigquery.Client(project=project)
  dataset_id = project + "." + dataset
  table_id = dataset_id + ".table_metadata"
  table = bigquery.Table(table_id)
  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=META_SUM_SCHEMA)

  f = open(filenm, "r")
  metaD = json.load(f)
  f.close()
  metaD.extend(cptacColRows)
  job = client.load_table_from_json(metaD, table, job_config=job_config)
  print(job.result())


def create_meta_table(project, dataset):

  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".column_metadata"

  schema = [
            bigquery.SchemaField("collection_id","STRING"),
            bigquery.SchemaField("case_col","BOOLEAN"),
            bigquery.SchemaField("table_name","STRING"),
            bigquery.SchemaField("column","STRING"),
            bigquery.SchemaField("column_label","STRING"),
            bigquery.SchemaField("data_type","STRING"),
            bigquery.SchemaField("original_column_headers","STRING", mode="REPEATED",
                ),
            bigquery.SchemaField("values", "RECORD", mode="REPEATED",
                fields=[
                  bigquery.SchemaField("option_code","STRING"),
                  bigquery.SchemaField("option_description","STRING"),
             ],
            ),
           bigquery.SchemaField("values_source","STRING"),
           bigquery.SchemaField("files", "STRING", mode="REPEATED"),
           bigquery.SchemaField("sheet_names","STRING",mode="REPEATED"),
           bigquery.SchemaField("batch", "INTEGER",mode="REPEATED"),
           bigquery.SchemaField("column_numbers", "INTEGER", mode="REPEATED")
           ] 
  
  dataset=bigquery.Dataset(dataset_id)
  dataset.location='US'
  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)

def load_meta(project, dataset, filenm,cptacRows):
  client = bigquery.Client(project=project)
  dataset_id = project+"."+dataset
  table_id = dataset_id+".column_metadata"
  table = bigquery.Table(table_id)

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

  with open(filenm,"rb") as source_file:
    f=open(filenm,'r')
    metaD=json.load(f)
    f.close()
    metaD.extend(cptacRows)
    job=client.load_table_from_json(metaD, table, job_config=job_config)
    print(job.result())

def checkData():  
  dataset_id=DEFAULT_PROJECT+'.'+DATASET
  client = bigquery.Client(project=DEFAULT_PROJECT)
  query = "select distinct idc_webapp_collection_id, PatientID from "+DICOM_META+" order by idc_webapp_collection_id"
  job = client.query(query)
  ids={}
  for row in job.result():
    colec=row['idc_webapp_collection_id']
    cid=row['PatientID']
    if not (colec in ids):
        print("Collecs "+colec)
        ids[colec]={}
    ids[colec][cid]=1


  tables= client.list_tables(dataset_id)
  tableNms=[tb.table_id for tb in tables]
  if ("table_metadata" in tableNms):
    tableNms.remove("table_metadata")
  else:
    print("table_metadata is missing!")
  if ("column_metadata" in tableNms):
    tableNms.remove("column_metadata")
  else:
    print("column_metadata is missing!")

  tableNms.sort()

  query = "select distinct table_name from "+dataset_id+".table_metadata "
  job = client.query(query)
  tableL = [row.table_name for row in job.result()]
  tableL = [x.split('.')[len(x.split('.'))-1] for x in tableL]
  tableL.sort()
  if not (tableNms == tableL):
    print("table_metadata list is incorrect")

  query = "select distinct table_name from " + dataset_id + ".column_metadata "
  job = client.query(query)
  tableL = [row.table_name for row in job.result()]
  tableL = [x.split('.')[len(x.split('.'))-1] for x in tableL]
  tableL.sort()
  if not (tableNms == tableL):
    print("column_metadata table list is incorrect")

  for tableNm in tableNms:
    table_id=dataset_id+'.'+tableNm
    colec=tableNm.rsplit('_',1)[0]
    print("colec "+colec)
    table=client.get_table(table_id)
    colNames=[col.name for col in table.schema]
    colNames.sort()
    final_id=FINAL_PROJECT+"."+CURRENT_VERSION+"_clinical."+tableNm 
    query = "select table_name,column from " + dataset_id + ".column_metadata where table_name= '"+final_id+"'"
    job = client.query(query)
    print(query)
    colL = [row.column for row in job.result()]
    colL.sort()
    if not (colNames == colL):
      print ("mismatch in columns for table "+tableNm+"!")
    i=1
    numExt=0
    curDic=ids[colec]
    query = "select distinct dicom_patient_id from " + table_id
    job = client.query(query)
    for row in job.result():
      cid=row['dicom_patient_id']
      if not (cid in curDic):
        numExt=numExt+1
    if (numExt>0):
      print("for table "+tableNm+ " "+str(numExt)+" ids not in dicom ")


def load_clin_files(project, dataset,cpath,srcfiles):
  error_sets=[]  
  client = bigquery.Client(project=project)
  ofiles=[]
  if srcfiles is None:
    ofiles = [f for f in listdir(cpath) if isfile(join(cpath,f))]
  else:
    ofiles=srcfiles
  dataset_created={}
  for ofile in ofiles:
    cfile= join(cpath,ofile)
    collec = splitext(ofile)[0]
    file_ext = splitext(ofile)[1]
    print(collec+" "+file_ext)
    if file_ext=='.csv':
        table_id=project+"."+dataset+"."+collec
        job_config=bigquery.LoadJobConfig(autodetect=True,source_format=bigquery.SourceFormat.CSV)
        print(cfile)
        with open(cfile,'rb') as nfile:
          job=client.load_table_from_file(nfile,table_id, job_config=job_config)
          print(job.result())
        nfile.close()   
    if file_ext=='.json':
      table_id =project+"."+dataset+"."+collec
      job_config= bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
      schema=[]
      f=open(cfile,'r')
      metaD=json.load(f)
      f.close()
      schemaD=metaD['schema']
      cdata=metaD['data']
      for nset in schemaD:
        col=nset[0]
        dtype=nset[1]
        colType="STRING"
        if dtype == "int":
          colType="INTEGER"
        elif dtype == "float":
          colType="FLOAT"
        schema.append(bigquery.SchemaField(col,colType))
      client.delete_table(table_id,not_found_ok=True)
      table=bigquery.Table(table_id)
      job_config =bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
      try:
        job= client.load_table_from_json(cdata, table, job_config=job_config)    
        print(job.result())
      except:
        error_sets.append(collec)        
  print('error sets')
  print(str(error_sets)) 


def load_all(project,dataset,version,last_dataset, last_version):
  client = bigquery.Client(project=project)
  dataset_id=project+"."+dataset
  ds = bigquery.Dataset(dataset_id)
  ds.location = 'US'
  client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
  ds = client.create_dataset(dataset_id)

  cptac=addTables(project, dataset, version, "CPTAC", None, "clinical", CPTAC_SRC, "submitter_id", False, last_dataset, last_version)
  tcga=addTables(project, dataset, version, "TCGA", None, "clinical", TCGA_SRC, "case_barcode", False, last_dataset, last_version)

  bqSrcMetaTbl = cptac[0]+tcga[0]
  bqSrcMetaCol = cptac[1]+tcga[1]

  create_meta_summary(project, dataset, bqSrcMetaTbl)
  create_meta_table(project, dataset)
  filenm = "./" + CURRENT_VERSION + "_table_metadata.json"
  load_meta_summary(project, dataset, bqSrcMetaTbl,filenm)

  filenm="./"+CURRENT_VERSION+"_column_metadata.json"
  load_meta(project,dataset,filenm,bqSrcMetaCol)

  dirnm="./clin_"+CURRENT_VERSION
  load_clin_files(project,dataset,dirnm,None)


if __name__=="__main__":
  load_all(DEFAULT_PROJECT, DATASET,CURRENT_VERSION, LAST_DATASET, LAST_VERSION)
  checkData()

