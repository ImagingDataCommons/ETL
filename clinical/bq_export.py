from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys
from addcptac import get_cptac, create_table_meta_cptac_row, create_column_meta_cptac_rows, copy_cptac

DEFAULT_SUFFIX='clinical'
DEFAULT_DESCRIPTION='clinical data'
DEFAULT_PROJECT ='idc-dev-etl'

CURRENT_VERSION = 'idc_v10'
DATASET=CURRENT_VERSION+'_clinical'

def create_meta_summary(project, dataset,cptac):
  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".table_metadata"
  filenm=CURRENT_VERSION+"_table_metadata.json"

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
              bigquery.SchemaField("added_md5","STRING"),
              bigquery.SchemaField("prior_md5","STRING"),
              bigquery.SchemaField("update_md5","STRING"),
              bigquery.SchemaField("table_last_modified", "STRING"),
              bigquery.SchemaField("table_size", "INTEGER"),
            ]  
          ),

           ] 

  dataset=bigquery.Dataset(dataset_id)
  dataset.location='US'
  client.delete_dataset(dataset_id,delete_contents=True,not_found_ok=True)
  dataset=client.create_dataset(dataset)
  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)
  job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema)
  f=open(filenm,"r")
  metaD=json.load(f)
  f.close()
  cptacRow = create_table_meta_cptac_row(cptac)
  metaD.extend(cptacRow)
  job=client.load_table_from_json(metaD, table, job_config=job_config)
  print(job.result())

def create_meta_table(project, dataset):

  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".column_metadata"

  schema = [
            bigquery.SchemaField("collection_id","STRING"),
            bigquery.SchemaField("case_col","BOOLEAN"),
            bigquery.SchemaField("table_name","STRING"),
            bigquery.SchemaField("variable_name","STRING"),
            bigquery.SchemaField("variable_label","STRING"),
            bigquery.SchemaField("data_type","STRING"),
            bigquery.SchemaField("original_column_headers","STRING", mode="REPEATED",
                ),
            bigquery.SchemaField("values", "RECORD", mode="REPEATED",
                fields=[
                  bigquery.SchemaField("option_code","STRING"),
                  bigquery.SchemaField("option_description","STRING"),
             ],
            ),
           bigquery.SchemaField("files", "RECORD", mode="REPEATED",
               fields=[
                   bigquery.SchemaField("name","STRING")
                   ],
               ),
           bigquery.SchemaField("sheet_names","STRING",mode="REPEATED"),
           bigquery.SchemaField("batch", "INTEGER",mode="REPEATED"),
           bigquery.SchemaField("column_numbers", "INTEGER", mode="REPEATED")
           ] 
  
  dataset=bigquery.Dataset(dataset_id)
  dataset.location='US'
  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)

def load_meta(project, dataset, filenm,cptac):
  client = bigquery.Client(project=project)
  table_id=project+"."+dataset+".column_metadata"
  table=bigquery.Table(table_id)

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

  with open(filenm,"rb") as source_file:
    f=open(filenm,'r')
    metaD=json.load(f)
    f.close()
    cptacRows = create_column_meta_cptac_rows(cptac)
    metaD.extend(cptacRows)
    #metaD=cptacRows
    job=client.load_table_from_json(metaD, table, job_config=job_config)
    print(job.result())

def load_clin_files(project, dataset,cpath):
  error_sets=[]  
  client = bigquery.Client(project=project)
  ofiles = [f for f in listdir(cpath) if isfile(join(cpath,f))]
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
      job_config =bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema)
      try:
        job= client.load_table_from_json(cdata, table, job_config=job_config)    
        print(job.result())
      except:
        error_sets.append(collec)        
  print('error sets')
  print(str(error_sets)) 

def load_all(project,dataset):
   cptac=get_cptac()
   create_meta_summary(project, dataset,cptac)
   copy_cptac()
   create_meta_table(project, dataset)
   filenm="./"+CURRENT_VERSION+"_column_metadata.json"
   load_meta(project,dataset,filenm,cptac)
   dirnm="./clin_"+CURRENT_VERSION
   load_clin_files(project,dataset,dirnm)


if __name__=="__main__":
  load_all(DEFAULT_PROJECT, DATASET)

