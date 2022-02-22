from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys


def create_meta_table(project, dataset):

  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".clinical_meta"

  schema = [
            bigquery.SchemaField("collection","STRING"),
            bigquery.SchemaField("case_col","BOOLEAN"),
            bigquery.SchemaField("table_name","STRING"),
            bigquery.SchemaField("sources", "STRING"),
            bigquery.SchemaField("column_number","INTEGER"),
            bigquery.SchemaField("variable_name","STRING"),
            bigquery.SchemaField("variable_label","STRING"),
            bigquery.SchemaField("data_type","STRING"),
            bigquery.SchemaField("num_values","INTEGER"),
            bigquery.SchemaField("values", "RECORD", mode="REPEATED",
                fields=[
                  bigquery.SchemaField("option_value","STRING"),
                  bigquery.SchemaField("option_description","STRING"),
                  bigquery.SchemaField("option_code","INTEGER")
             ],
            ),
           bigquery.SchemaField("rng","STRING") 
           ] 
  
  dataset=bigquery.Dataset(dataset_id)
  dataset.location='US'
  client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
  dataset = client.create_dataset(dataset)
  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)

def load_meta(project, dataset, filenm):
  client = bigquery.Client(project=project)
  table_id=project+"."+dataset+".clinical_meta"
  table=bigquery.Table(table_id)

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

  with open(filenm,"rb") as source_file:
    f=open(filenm,'r')
    metaD=json.load(f)
    f.close()
    for row in metaD:
      row['sources']=json.dumps(row['sources'])
      if 'rng' in row:
          row['rng']=str(row['rng'])
    job=client.load_table_from_json(metaD, table, job_config=job_config)
    print(job.result())

def load_clin_files(project, dataset,cpath,use_schema):
  client = bigquery.Client(project=project)
  ofiles = [f for f in listdir(cpath) if isfile(join(cpath,f))]
  for ofile in ofiles:
    cfile= join(cpath,ofile)
    collec = splitext(ofile)[0]
    collec=collec.replace('/','_')
    collec=collec.replace('-', '_')
    collec=collec.replace(' ', '_')
    collec=collec.replace('_','')

    file_ext = splitext(ofile)[1]
    if file_ext=='.json':
      table_id =project+"."+dataset+"."+collec+"_clinical"
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
      if use_schema: 
        table=bigquery.Table(table_id, schema=schema)
      job= client.load_table_from_json(cdata, table, job_config=job_config)    
      print(job.result())
    
def load_all(project,dataset,use_schema):
  create_meta_table(project, dataset)
  load_meta(project,dataset,"./clinical_meta_out.json")
  load_clin_files(project,dataset,"./clin/")


if __name__=="__main__":
  project=sys.argv[1]
  dataset=sys.argv[2]
  use_schema = sys.argv[3]
  load_all(project,dataset,use_schema)

