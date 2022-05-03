from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys

DEFAULT_SUFFIX='clinical'
DEFAULT_DESCRIPTION='clinical data'
DEFAULT_PROJECT ='idc-dev-etl'
CURRENT_VERSION = 'idc_v9'
DATASET=CURRENT_VERSION+'_tmpclinical'


def create_meta_summary(project, dataset):
  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".clinical_meta_table"
  filenm=CURRENT_VERSION+"_clinical_meta_table.json"

  schema = [
          bigquery.SchemaField("collection_id","STRING"),
          bigquery.SchemaField("table_name","STRING"),
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
            ]  
          ),
          bigquery.SchemaField("dataset","STRING"),
          bigquery.SchemaField("project","STRING")
           ] 
         

  dataset=bigquery.Dataset(dataset_id)
  dataset.location='US'
  client.delete_dataset(dataset_id,delete_contents=True,not_found_ok=True)
  dataset=client.create_dataset(dataset)

  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)
  job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema)
  #filenm="temp.json"
  f=open(filenm,"r")
  metaD=json.load(f)
  f.close()
  job=client.load_table_from_json(metaD, table, job_config=job_config)
  print(job.result())

def create_meta_table(project, dataset):

  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".clinical_meta_column"

  schema = [
            bigquery.SchemaField("collection_id","STRING"),
            bigquery.SchemaField("case_col","BOOLEAN"),
            bigquery.SchemaField("table_name","STRING"),
            bigquery.SchemaField("table_description", "STRING"),
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

def load_meta(project, dataset, filenm):
  client = bigquery.Client(project=project)
  table_id=project+"."+dataset+".clinical_meta_column"
  table=bigquery.Table(table_id)

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

  with open(filenm,"rb") as source_file:
    f=open(filenm,'r')
    metaD=json.load(f)
    f.close()
    job=client.load_table_from_json(metaD, table, job_config=job_config)
    print(job.result())

def load_clin_files(project, dataset,cpath):
  client = bigquery.Client(project=project)
  ofiles = [f for f in listdir(cpath) if isfile(join(cpath,f))]
  dataset_created={}
  for ofile in ofiles:
    cfile= join(cpath,ofile)
    collec = splitext(ofile)[0]
    file_ext = splitext(ofile)[1]
    print(collec+" "+file_ext)
    if file_ext=='.csv':
        #rcollecSp=ofile[::-1].split('_',1)
        #dataset_nm=rcollecSp[1][::-1]
        #collec=rcollecSp[0][::-1].replace('.csv','')

        #if not dataset_nm in dataset_created:
        #  dataset_id=project+"."+dataset_nm
        #  cdataset = bigquery.Dataset(dataset_id)
        #  cdataset.location='US'
        #  client.delete_dataset(dataset_id,delete_contents=True,not_found_ok=True)
        #  cdataset=client.create_dataset(cdataset)
        #  dataset_created[dataset_nm]=1
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
      job= client.load_table_from_json(cdata, table, job_config=job_config)    
      print(job.result())
    
def load_all(project,dataset):
   #pass
   create_meta_summary(project, dataset) 
   create_meta_table(project, dataset)
   filenm="./"+CURRENT_VERSION+"_clinical_meta_column.json"
   #filenm="./ntmp2.json"
   print(filenm)
   load_meta(project,dataset,filenm)
   dirnm="./clin_"+CURRENT_VERSION
   load_clin_files(project,dataset,dirnm)


if __name__=="__main__":
  #client = bigquery.Client(project="idc-dev-etl")
  #dataset_id="idc-dev-etl.acrin_nlsc_fdg_pet_ct"
  #dataset=bigquery.Dataset(dataset_id)
  #dataset.location='US'
  #client.delete_dataset(dataset_id,delete_contents=True,not_found_ok=True)
  load_all(DEFAULT_PROJECT, DATASET)

