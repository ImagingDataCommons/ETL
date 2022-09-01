from google.cloud import bigquery
import json


def read_clin_file(filenm):
  f =open(filenm,'r')
  clinJson=json.load(f)
  f.close()
  return clinJson


def getSumDic(CURRENT_VERSION,src_info,num_bstches,propost_process_src,post_process_src_current_md5):
  sumDic['collection_id'] = collection_id
  sumDic['table_name'] = table_name
  sumDic['post_process_src'] = post_process_src

  if table_name in hist:
    for nkey in hist[table_name]:
      if (nkey not in sumDic) and not (nkey == 'source_info'):
        sumDic[nkey] = hist[table_name][nkey]
    if (hist[table_name]['post_process_src'] != post_process_src) or (
            post_process_src_current_md5 != hist[table_name]['post_process_src_updated_md5']):
      sumDic['idc_version_table_prior'] = sumDic['idc_version_table_updated']
      sumDic['idc_version_table_prior_md5'] = sumDic['idc_version_table_updated_md5']
      sumDic['idc_version_table_updated'] = CURRENT_VERSION
      sumDic['idc_version_table_updated_md5'] = post_process_src_current_md5
      for i in range(len(src_info)):
        if (i < len(hist[table_name]['source_info'])) and (
                src_info[i]['srcs'][0] == hist[table_name]['source_info']['srcs'][0]):
          src_info[i]['added_md5'] = hist[table_name]['source_info'][i]['added_md5']
          if src_info[i]['update_md5'] == hist[table_name]['source_info'][i]['update_md5']:
            src_info[i]['prior_md5'] = hist[table_name]['source_info'][i]['prior_md5']
          else:
            src_info[i]['prior_md5'] = hist[table_name]['source_info'][i]['update_md5']
        else:
          src_info[i]['added_md5'] = src_info[i]['update_md5']
          src_info[i]['prior_md5'] = src_info[i]['prior_md5']
  else:
    sumDic['idc_version_table_added'] = CURRENT_VERSION
    sumDic['table_added_datetime'] = str(datetime.now(pytz.utc))
    # sumDic['post_process_src']=post_process_src
    sumDic['post_process_src_added_md5'] = post_process_src_current_md5
    sumDic['idc_version_table_prior'] = CURRENT_VERSION
    sumDic['post_process_src_prior_md5'] = post_process_src_current_md5
    sumDic['idc_version_table_updated'] = CURRENT_VERSION
    sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
    sumDic['post_process_src_updated_md5'] = post_process_src_current_md5
    sumDic['number_batches'] = num_batches
    for i in range(len(src_info)):
      src_info[i]['added_md5'] = src_info[i]['update_md5']
      src_info[i]['prior_md5'] = src_info[i]['update_md5']

  sumDic['source_info'] = src_info
  sumDic['project'] = project
  sumDic['dataset'] = dataset

def getHist(hist,table_id):
  client = bigquery.Client()
  query = "select * from `" + table_id + "`"
  try:
    job = client.query(query)
    for row in job.result():
      nmInd = row['table_name'].split('.')
      tbl = nmInd[len(nmInd)-1]
      cdic={}
      cdic['idc_version_table_added'] = row['idc_version_table_added']
      cdic['table_added_datetime'] = row['table_added_datetime']
      cdic['post_process_src'] = row['post_process_src']
      cdic['post_process_src_added_md5'] = row['post_process_src_added_md5']
      cdic['idc_version_table_prior'] = row['idc_version_table_prior']
      cdic['post_process_src_prior_md5'] = row['post_process_src_prior_md5']
      cdic['idc_version_table_updated'] = row['idc_version_table_updated']
      cdic['post_process_src_updated_md5'] = row['post_process_src_updated_md5']
      cdic['number_batches'] = row['number_batches']
      cdic['source_info']=row['source_info']
      hist[tbl]=cdic
  except:
    pass