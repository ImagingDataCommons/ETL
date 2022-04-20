from google.cloud import bigquery
import json
import re
import pandas as pd
import numpy as np
import re
import sys
from os import path, listdir,mkdir
import zipfile
import acrin_forms
import shutil
from pathlib import Path
import hashlib
import pytz
from datetime import datetime
#import copy.deepcopy

ORIGINAL_SRCS_PATH= '/Users/george/fed/actcianable/output/clinical_files/'
NOTES_PATH = '/Users/george/fed/actcianable/output/'
DEFAULT_SUFFIX='clinical'
DEFAULT_DESCRIPTION='clinical data'
DEFAULT_DATASET ='idc_v9_clinical'
DEFAULT_PROJECT ='idc-dev-etl'
CURRENT_VERSION = 'idc_v9'
LAST_VERSION = 'idc_v8'
LAST_DATASET = 'idc_v8_clinical'
DESTINATION_FOLDER='./clin_'+CURRENT_VERSION+'/'

def get_md5(filenm):
  with open(filenm, 'rb') as file_to_check:
    # read contents of the file
    data = file_to_check.read()
    # pipe contents of the file through
    return(hashlib.md5(data).hexdigest())

def write_dataframe_to_json(path,nm,df):
  #headers = clinJson[coll]['headers']
  filenm=path+nm+'.json'
  f = open(filenm, 'w')
  cols=list(df.columns)
  nArr = []
  for i in range(len(cols)):
    col=df.columns[i]
    dtype=df.dtypes[i].name
    ntype=''
    if dtype=='object':
      ntype='str'
    elif dtype=='float64':
      ntype='float'
    elif dtype=='int64':
      ntype='int'
    elif dtype == 'datetime64[ns]':
      ntype='datetime'
      df[df.columns[i]] = df[df.columns[i]].astype(str)
    elif dtype == 'bool':
      ntype = 'bool'
    else:
      ntype = 'str'
      df[df.columns[i]] = df[df.columns[i]].astype(str)
    nArr.append([col, ntype])
  data = [{**row.dropna().to_dict()} for index, row in df.iterrows()]
  out={'schema':nArr, 'data':data}
  try:
    json.dump(out,f)
  except:
    pass
  f.close()


def read_clin_file(filenm):
  f =open(filenm,'r')
  clinJson=json.load(f)
  f.close()
  return clinJson

def write_clin_file(filenm, data):

  for curKey in clinJson:
    ndata = clinJson[curKey]
    f = open(filenm, 'w')
    json.dump(ndata, f)
    f.close()

def recastDataFrameTypes(df, ptId):
  for i in range(len(df.columns)):
    if not (i == ptId) and (df.dtypes[i].name == 'float64'):
      try:
        df[df.columns[i]] = df[df.columns[i]].astype('Int64')
      except:
        pass
    # make all not na objects strings
    if (df.dtypes[i].name == 'object'):
      try:
        df[df.columns[i]] = df[df.columns[i]].map(lambda a: a if pd.isna(a) else str(a))
      except:
        pass

def analyzeDataFrame(cdic):
  df = cdic['df']
  #last df column is idc_batch, added by us
  for i in range(len(df.columns)-1):
    try:
      uVals = list(df[df.columns[i]].dropna().unique())
    except:
      pass
    try:
      uVals.sort()
    except:
      pass
    cdic['headers'][df.columns[i]][0]['uniques']=uVals
    if (df.dtypes[i].name == 'float64') or (df.dtypes[i].name == 'Int64'):
      if (len(uVals)>0):
        cdic['headers'][df.columns[i]][0]['rng']=[float(uVals[0]),float(uVals[len(uVals)-1])]
        iii=1

def processSrc(fpath, colName, srcInfo):
  attrs=[]
  filenm = fpath+colName+'/'+srcInfo['filenm']
  sheetNo = (0 if not 'sheet' in srcInfo else srcInfo['sheet'])
  patientIdRow = (0 if not ('patientIdRow') in srcInfo else srcInfo['patientIdRow'])
  rows = ([0] if not 'headrows' in srcInfo else srcInfo['headrows'])
  skipRows = (None if not 'skipRows' in srcInfo else srcInfo['skipRows'])
  skipCols = (None if not 'skipCols' in srcInfo else srcInfo['skipCols'])
  pivot= (False if not 'pivot' in srcInfo else srcInfo['pivot'])
  maxRow = (-1 if not 'maxRow' in srcInfo else srcInfo['maxRow'])
  extension = path.splitext(filenm)[1]
  engine='xlrd'
  if extension == '.xlsx':
    engine= 'openpyxl'
  elif extension == '.xlsb':
    engine = 'pyxlsb'
  df=[]
  if extension == '.csv':
    df = pd.read_csv(filenm)
    sheetnm=''
  else:
    dfi = pd.read_excel(filenm, engine=engine, sheet_name=None)
    sheetnm = list(dfi.keys())[sheetNo]
    df = dfi[sheetnm]
  if pivot:
    df = df.T
    rows =[rows[i]+1 for i in range(len(rows))]
    colList=list(df.columns)
    df.insert(0,'tmp',list(df.index))

  if skipCols is not None:
    df.drop(columns=[df.columns[i] for i in skipCols],inplace=True)

  for i in range(len(df.columns)):
    attrs.append([])

  for i in range(len(rows)):
    colVal=''
    ind = rows[i]
    if ind == 0:
      values = df.columns
    else:
      values=df.values[ind-1]
    for j in range(len(values)):
      val=values[j]
      if (i == len(rows)-1) or (not (str(val) == 'nan') and not ('Unnamed:' in str(colVal))):
        colVal=val
      if (i < (len(rows)-1)) or (not (str(colVal) == 'nan') and not ('Unnamed:' in str(colVal))):
        attrs[j].append(colVal)

  drrows=[i-1 for i in rows]
  if skipRows is not None:
    skipRows = [skipRows[i]-1 for i in range(len(skipRows))]
    drrows =drrows+skipRows

  if maxRow>-1:
    drrows=drrows+[i for i in range(maxRow,len(list(df.index)))]
  if -1 in drrows:
    drrows.remove(-1)
  df.drop(df.index[drrows], inplace=True)
  headers = formatForBQ(attrs,lc=True)
  df.columns=headers
  df.index=list(df.iloc[:,patientIdRow])

  headerSet = {}
  for i in range(len(headers)):
    headerSet[headers[i]]={"attrs":attrs[i],"colNo":i}

  if ("reindex" in srcInfo) and ("needed" in srcInfo["reindex"]) and srcInfo["reindex"]["needed"]:
    uniques = srcInfo["reindex"]["uniques"]
    df_new=pd.DataFrame()
    #df_new.index=df.index
    #df_new.columns=list(df.columns)
    newInd={}
    pos=0
    for i in range(df.shape[0]):
      curInd=df.index[i]
      if not (curInd in newInd):
        df_new = df_new.append(df.iloc[i])
        newInd[curInd]=pos
        pos=pos+1
      else:
        for colInd in range(len(df.columns)):
          if not (colInd==patientIdRow) and not (colInd in uniques):
            curVal= df_new.iloc[newInd[curInd]][colInd]
            addVal =  df.iloc[i][colInd]
            #df_new.loc[curInd, list(df.columns)[colInd]]=10
            df_new.loc[curInd, list(df.columns)[colInd]]=str(curVal)+", "+str(addVal)
    df = pd.concat([df_new])
  try:
    df[df.columns[patientIdRow]] = df[df.columns[patientIdRow]].astype('Int64')
  except:
    df[df.columns[patientIdRow]] = df[df.columns[patientIdRow]].astype('str')
  return [headerSet,df,sheetnm]

def formatForBQ(attrs, lc=False):
  patt=re.compile(r"[a-zA-Z_0-9]")
  justNum=re.compile(r"[0-9]")
  headcols=[]
  for i in range(len(attrs)):
    headSet=attrs[i]
    header='_'.join(str(k) for k in headSet)
    header=header.replace('/','_')
    header=header.replace('-', '_')
    header=header.replace(' ', '_')
    normHeader = ''
    for i in range(len(header)):
      if bool(patt.search(header[i])):
        normHeader = normHeader + header[i]
    if (len(normHeader) > 0) and bool(justNum.search(normHeader[0])):
      normHeader='c_'+normHeader
    if lc:
      normHeader = normHeader.lower()

    headcols.append(normHeader)
  return headcols

def mergeAcrossAttr(clinJson, coll):
  mbatch = clinJson[coll]['mergeBatch']
  headers= {}
  ptIdSeq=[]
  ptId = mbatch[0]['ptId'][0][1]
  try:
    new_df=pd.concat([mbatch[0]['df']])
  except:
    print("could not concate! "+coll)
    new_df = mbatch[0]['df']
  for i in range(len(mbatch)):
    ptIdSeq.append(mbatch[i]['ptId'])
    cptRow = mbatch[i]['ptId'][0][0]
    cptId = mbatch[i]['ptId'][0][1]
    cheaders = mbatch[i]['headers']
    for chead in cheaders:
      if not chead in headers and ((not chead == cptId) or (i == 0)):
        headers[chead] = {}
        headers[chead]['srcs']=[]
      if (chead == cptId):
        headers[ptId]['srcs'].append(mbatch[i]['headers'][chead])
      else:
        headers[chead]['srcs'].append(mbatch[i]['headers'][chead])
    if (i > 0):
      nxt_df = mbatch[i]['df']
      cols = list(nxt_df.columns)
      cols[cptRow] = ptId
      nxt_df.columns=cols
      #new_df = pd.concat([new_df, nxt_df])
      try:
        new_df = pd.concat([new_df, nxt_df])
      except:
        print("could not concate! " + coll)

  clinJson[coll]['headers'] = headers
  clinJson[coll]['ptIdSeq'] = ptIdSeq
  clinJson[coll]['df'] = new_df

def mergeAcrossBatch(clinJson,coll,ptRowIds,attrSetInd):
  if 'mergeBatch' not in clinJson[coll]:
    clinJson[coll]['mergeBatch'] = []
  clinJson[coll]['mergeBatch'].append({})
  cList = list(clinJson[coll]['cols'][attrSetInd][0]['df'].columns)
  ptRow = cList[ptRowIds[0]]
  clinJson[coll]['mergeBatch'][attrSetInd]['ptId'] = []
  clinJson[coll]['mergeBatch'][attrSetInd]['ptId'].append([ptRowIds[0],ptRow])

  clinJson[coll]['mergeBatch'][attrSetInd]['headers'] = {}

  for header in clinJson[coll]['cols'][attrSetInd][0]['headers']:
    clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header] = [clinJson[coll]['cols'][attrSetInd][0]['headers'][header]]
    clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header][0]['filenm'] = clinJson[coll]['srcs'][attrSetInd][0]['filenm']
    if 'sheet' in clinJson[coll]['srcs'][attrSetInd][0]:
      clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header][0]['sheet'] = clinJson[coll]['srcs'][attrSetInd][0]['sheet']
    if 'sheetnm' in clinJson[coll]['srcs'][attrSetInd][0]:
      clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header][0]['sheetnm'] = clinJson[coll]['srcs'][attrSetInd][0]['sheetnm']
    clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header][0]['batch'] = 0

  clinJson[coll]['mergeBatch'][attrSetInd]['srcs']=[]
  clinJson[coll]['mergeBatch'][attrSetInd]['srcs'].append([])
  if 'archive' in clinJson[coll]['srcs'][attrSetInd][0]:
    clinJson[coll]['mergeBatch'][attrSetInd]['srcs'][0]=clinJson[coll]['mergeBatch'][attrSetInd]['srcs'][0]+clinJson[coll]['srcs'][attrSetInd][0]['archive']
  clinJson[coll]['mergeBatch'][attrSetInd]['srcs'][0].append(clinJson[coll]['srcs'][attrSetInd][0]['filenm'])

  df_all_rows =  pd.concat([clinJson[coll]['cols'][attrSetInd][0]['df']])


  for batchSetInd in range(1,len(clinJson[coll]['cols'][attrSetInd])):
    nList = list(clinJson[coll]['cols'][attrSetInd][batchSetInd]['df'].columns)
    cptRow = nList[ptRowIds[batchSetInd]]
    clinJson[coll]['mergeBatch'][attrSetInd]['ptId'].append([ptRowIds[batchSetInd],cptRow])
    if not ptRow == cptRow:
      print("Different patientColumn! "+coll)
    for colInd in range(len(nList)):
      col = nList[colInd]
      if not (col in cList) and not (col == cptRow):
        cList.append(col)
    for header in clinJson[coll]['cols'][attrSetInd][batchSetInd]['headers']:
      if not header in clinJson[coll]['mergeBatch'][attrSetInd]['headers'] and not header == cptRow:
        clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header] = []
      ckey = header
      if header == cptRow:
        ckey = ptRow
        clinJson[coll]['mergeBatch'][attrSetInd]['headers'][ptRow].append(clinJson[coll]['cols'][attrSetInd][batchSetInd]['headers'][cptRow])
      else:
        clinJson[coll]['mergeBatch'][attrSetInd]['headers'][header].append(clinJson[coll]['cols'][attrSetInd][batchSetInd]['headers'][header])
      curInd=len(clinJson[coll]['mergeBatch'][attrSetInd]['headers'][ckey])-1
      clinJson[coll]['mergeBatch'][attrSetInd]['headers'][ckey][curInd]['filenm']=clinJson[coll]['srcs'][attrSetInd][batchSetInd]['filenm']

      if 'sheet' in clinJson[coll]['srcs'][attrSetInd][batchSetInd]:
        clinJson[coll]['mergeBatch'][attrSetInd]['headers'][ckey][curInd]['sheet'] = clinJson[coll]['srcs'][attrSetInd][batchSetInd]['sheet']
      if 'sheetnm' in clinJson[coll]['srcs'][attrSetInd][batchSetInd]:
        clinJson[coll]['mergeBatch'][attrSetInd]['headers'][ckey][curInd]['sheetnm'] = clinJson[coll]['srcs'][attrSetInd][batchSetInd]['sheetnm']
      clinJson[coll]['mergeBatch'][attrSetInd]['headers'][ckey][curInd]['batch']=batchSetInd


    clinJson[coll]['mergeBatch'][attrSetInd]['srcs'].append([])
    if 'archive' in clinJson[coll]['srcs'][attrSetInd][batchSetInd]:
      clinJson[coll]['mergeBatch'][attrSetInd]['srcs'][batchSetInd] = clinJson[coll]['mergeBatch'][attrSetInd]['srcs'][batchSetInd] + clinJson[coll]['srcs'][attrSetInd][batchSetInd]['archive']
    clinJson[coll]['mergeBatch'][attrSetInd]['srcs'][batchSetInd].append(clinJson[coll]['srcs'][attrSetInd][batchSetInd]['filenm'])
    # join data frames
    new_df = clinJson[coll]['cols'][attrSetInd][batchSetInd]['df']

    #make sure joining df is using the same patientId column name as the original
    colList=list(new_df.columns)
    colList[ptRowIds[batchSetInd]] = ptRow
    new_df.columns = colList
    df_all_rows=pd.concat([df_all_rows,new_df])
  clinJson[coll]['mergeBatch'][attrSetInd]['cList'] = cList
  clinJson[coll]['mergeBatch'][attrSetInd]['df'] = df_all_rows

def export_meta_to_json(clinJson,filenm_meta,filenm_summary):
  metaArr=[]
  sumArr=[]
  for coll in clinJson:
    if 'dataset' in clinJson[coll]:
      dataset=clinJson[coll]['dataset']
    else:
      dataset = DEFAULT_DATASET

    if 'project' in clinJson[coll]:
      project = clinJson[coll]
    else:
      project = DEFAULT_PROJECT

    if ('idc_webapp' in clinJson[coll]) and ('mergeBatch' in clinJson[coll]):
      for k in range(len(clinJson[coll]['mergeBatch'])):
        if 'ptId' in clinJson[coll]['mergeBatch'][k]:
          sumDic = {}
          curDic=clinJson[coll]['mergeBatch'][k]
          curDf=clinJson[coll]['mergeBatch'][k]['df']
          dtypeL=list(curDf.dtypes)
          ptId=curDic['ptId'][0][0]

          if 'tabletypes' in clinJson[coll]:
            suffix=list(clinJson[coll]['tabletypes'][k].keys())[0]
            table_description = clinJson[coll]['tabletypes'][k][suffix]
          else:
            suffix=DEFAULT_SUFFIX
            table_description=DEFAULT_DESCRIPTION
          collection_id=clinJson[coll]['idc_webapp']
          table_name = collection_id + '_' + suffix
          try:
            post_process_src = './'+DESTINATION_FOLDER+'/'+clinJson[coll]['mergeBatch'][k]['outfile']
            post_process_src_current_md5 = get_md5(post_process_src)
          except:
            pass
          num_batches = len(clinJson[coll]['mergeBatch'][k]['srcs'])

          sumDic['collection_id']=collection_id
          sumDic['table_name']=table_name
          sumDic['idc_version_collection_added']=CURRENT_VERSION
          sumDic['collection_added_datetime']=str(datetime.now(pytz.utc))
          sumDic['post_process_src']=post_process_src
          sumDic['post_process_src_current_md5']=post_process_src_current_md5
          sumDic['number_batches']=num_batches

          src_info = []
          for src in clinJson[coll]['mergeBatch'][k]['srcs']:
            nsrc={}
            nsrc['srcs'] = src
            rootfile=ORIGINAL_SRCS_PATH+coll+'/'+src[0]
            nsrc['root_src_md5'] = get_md5(rootfile)
            src_info.append(nsrc)
          sumDic['source_info']=src_info
          sumArr.append(sumDic)

          for i in range(len(curDf.columns)):
            ndic = {}
            if (i == ptId):
              ndic['case_col']='yes'
            else:
              ndic['case_col'] = 'no'
            ndic['collection_id'] = collection_id
            ndic['table_name'] = table_name
            ndic['table_description'] = table_description
            header = curDf.columns[i]
            headerD = curDic['headers'][header][0]
            dftype=str(dtypeL[i].name)

            try:
              ndic['variable_label']=headerD['attrs'][len(headerD['attrs'])-1]
            except:
              pass
            if (dftype=='Object') or (dftype=='object'):
              dftype = 'String'

            ndic['variable_name']=str(header)
            ndic['data_type']=dftype
            if 'dictinfo' in headerD:
              ndic['variable_label']=headerD['dictinfo']['variable_label']
              ndic['data_type'] = headerD['dictinfo']['data_type']
              ndic['values']=headerD['dictinfo']['values']
            elif 'uniques' in headerD:
              num_values=len(headerD['uniques'])
              if (num_values<21):
                #ndic['uniques'] = headerD['uniques']
                ndic['values']=[]
                for val in headerD['uniques']:
                  ndic['values'].append({'option_code':str(val)})
              headerD.pop('uniques')
            ndic['original_column_headers'] = []
            ndic['files'] = []
            ndic['column_numbers'] = []
            ndic['sheet_names'] = []
            ndic['batch'] = []
            #sheetnms=[]
            for headerInfo in curDic['headers'][header]:
              ndic['original_column_headers'].append( str(headerInfo['attrs']))
              ndic['column_numbers'].append(headerInfo['colNo'])
              ndic['batch'].append(headerInfo['batch'])
              try:
                ndic['sheet_names'].append(headerInfo['sheetnm'])
              except:
                ndic['sheet_names'].append('')
                pass
              try:
                ndic['files'].append( {'name': headerInfo['filenm']} )
              except:
                pass
              ndic['project'] = project
              ndic['dataset'] = dataset

            metaArr.append(ndic)

  f=open(filenm_summary,'w')
  json.dump(sumArr,f)
  f.close()

  f = open(filenm_meta, 'w')
  json.dump(metaArr, f)
  f.close()


def parse_acrin_collection(clinJson,coll):
  webapp_coll=clinJson[coll]['idc_webapp']
  clinJson[coll]['mergeBatch']=[]
  clinJson[coll]['tabletypes']=[]
  #clinJson[coll]['dataset'] = webapp_coll+'_clinical'


  if 'uzip' in clinJson[coll]:
    for zpfile in clinJson[coll]['uzip']:
      zpfile = ORIGINAL_SRCS_PATH  + coll + '/' + zpfile
      with zipfile.ZipFile(zpfile) as zip_ref:
        zip_ref.extractall(ORIGINAL_SRCS_PATH  + coll)
    if 'udir' in clinJson[coll]:
      cdir = clinJson[coll]['udir'][0]
    else:
      cdir= path.splitext(clinJson[coll]['uzip'][0])[0]
    npath = ORIGINAL_SRCS_PATH + coll + '/' +cdir
    ofiles = [f for f in listdir(npath) if path.isfile(path.join(npath,f))]
    dictFile = [f for f in ofiles if 'Dictionary' in f][0]

    formFileA = [f for f in ofiles if ('Form' in f) and ('.xls' in f)]
    if len(formFileA)>0:
      formFile=npath+'/'+formFileA[0]
    else:
      formFile = None
    dictFile= npath+'/'+dictFile
    if 'dictfile' in clinJson[coll]:
      dictFile=ORIGINAL_SRCS_PATH + coll + '/' + clinJson[coll]['dictfile']
    parser=acrin_forms.DictionaryReader(dictFile,formFile)
    parser.parse_dictionaries()
    dict_names = parser.get_dictionary_names()
    for form_id in dict_names:
      desc = parser.get_dictionary_desc(form_id)
      cdict = parser.get_dictionary(form_id)
      reformCdict={}
      for celem in cdict:
        celem['variable_name']=celem['variable_name'].lower()
        reformCdict[celem['variable_name']]=celem

      srcf=npath+'/'+form_id+'.csv'
      if path.exists(srcf):
        print(srcf)

        clinJson[coll]['tabletypes'].append({form_id:desc})
        df = pd.read_csv(srcf)
        colnames = [list(df.columns)]
        if len(clinJson[coll]['uzip'])>1:
          if 'udir' in clinJson[coll]:
            ccdir = clinJson[coll]['udir'][1]
          else:
            ccdir = path.splitext(clinJson[coll]['uzip'][1])[0]
          osrcf= ORIGINAL_SRCS_PATH + coll + '/' + ccdir + '/' +form_id+'.csv'
          df2 = pd.read_csv(osrcf)
          colnames.append(list(df.columns))
          df = pd.concat([df, df2])
        #shutil.copy2(srcf,destf)
        recastDataFrameTypes(df, 0)
        destf = DESTINATION_FOLDER +'/' + webapp_coll + '_' + form_id + '.csv'
        df.to_csv(destf, index=False)
        ndic={}
        ndic['df']=df
        ndic['ptId']=[[0, df.columns[0].lower()]]
        ndic['headers'] ={}
        orig_names=list(df.columns)
        norm_names=[df.columns[k].lower() for k in range(len(df.columns))]
        df.columns=norm_names
        for k in range(len(df.columns)):
          headval = df.columns[k]
          ndic['headers'][headval] = []
          #df.columns[k]=df.columns[k].lower()
          orig_nm=orig_names[k]
          for kk in range(len(colnames)):
            if orig_nm in colnames[kk]:
              ind = colnames[kk].index(orig_nm)
              hndic={}
              hndic['attrs']=[orig_nm]
              hndic['colNo'] = ind
              hndic['sheet'] = 0
              if kk == 0:
                hndic['filenm'] = cdir+'/'+form_id + '.csv'
              else:
                hndic['filenm'] = ccdir + '/' + form_id + '.csv'
              hndic['batch'] =kk
            if (headval in reformCdict) and (len(ndic['headers'][headval])==0):
              hndic['dictinfo'] = reformCdict[headval]
            ndic['headers'][headval].append(hndic)
        ndic['srcs'] = [[clinJson[coll]['uzip'][0], cdir + '/' + form_id + '.csv']]
        if (len(clinJson[coll]['uzip'])>1):
          ndic['srcs'].append([clinJson[coll]['uzip'][1],ccdir + '/' + form_id + '.csv'])
        ndic['outfile']=webapp_coll + '_' + form_id + '.csv'
        clinJson[coll]['mergeBatch'].append(ndic)

  pass

def parse_conventional_collection(clinJson,coll):
  if 'uzip' in clinJson[coll]:
    zpfile = ORIGINAL_SRCS_PATH + coll + '/' + clinJson[coll]['uzip']
    with zipfile.ZipFile(zpfile) as zip_ref:
      zip_ref.extractall(ORIGINAL_SRCS_PATH + coll)
  if 'srcs' in clinJson[coll]:
    clinJson[coll]['cols'] = []
    for attrSetInd in range(len(clinJson[coll]['srcs'])):
      ptRowIds = []
      cohortSeries = clinJson[coll]['srcs'][attrSetInd]
      clinJson[coll]['cols'].append([])
      wJson = False
      for batchSetInd in range(len(clinJson[coll]['srcs'][attrSetInd])):
        clinJson[coll]['cols'][attrSetInd].append([])
        clinJson[coll]['cols'][attrSetInd][batchSetInd] = {}
        srcInfo = clinJson[coll]['srcs'][attrSetInd][batchSetInd]
        patientIdRow = (0 if not ('patientIdRow') in srcInfo else srcInfo['patientIdRow'])
        ptRowIds.append(patientIdRow)
        print("strcInfo " + str(srcInfo))
        if not ('type' in srcInfo) or not (srcInfo['type'] == 'json'):
          [headers, df, sheetnm] = processSrc(ORIGINAL_SRCS_PATH, coll, srcInfo)
          df['source_batch'] = batchSetInd
          headers['source_batch'] = {'attrs':['NA'], 'colNo':-1}
          # attrs.append([attr])
          srcInfo['sheetnm']=sheetnm
          clinJson[coll]['cols'][attrSetInd][batchSetInd]['headers'] = headers
          clinJson[coll]['cols'][attrSetInd][batchSetInd]['df'] = df
        else:
          wJson = True
      if not wJson and 'idc_webapp' in clinJson[coll]:
        mergeAcrossBatch(clinJson, coll, ptRowIds, attrSetInd)
        recastDataFrameTypes(clinJson[coll]['mergeBatch'][attrSetInd]['df'],
                             clinJson[coll]['mergeBatch'][attrSetInd]['ptId'][0][0])
        analyzeDataFrame(clinJson[coll]['mergeBatch'][attrSetInd])
        suffix = DEFAULT_SUFFIX
        if 'tabletypes' in clinJson[coll]:
          suffix = list(clinJson[coll]['tabletypes'][attrSetInd].keys())[0]
        nm = clinJson[coll]['idc_webapp'] + '_' + suffix
        clinJson[coll]['mergeBatch'][attrSetInd]['outfile'] = nm + '.json'
        write_dataframe_to_json(DESTINATION_FOLDER, nm, clinJson[coll]['mergeBatch'][attrSetInd]['df'])

    if not wJson and 'idc_webapp' in clinJson[coll]:
      mergeAcrossAttr(clinJson, coll)
      # recastDataFrameTypes(clinJson[coll]['df'], clinJson[coll]['ptIdSeq'][0][0][0])
      # analyzeDataFrame(clinJson[coll])
      # write_dataframe_to_json('./clin/',coll,clinJson)

if __name__=="__main__":
  dirpath = Path(DESTINATION_FOLDER)
  if dirpath.exists() and dirpath.is_dir():
    shutil.rmtree(dirpath)
  mkdir(dirpath)

  #ORIGINAL_SRCS_PATH=sys.argv[1]
  clinJson =read_clin_file(NOTES_PATH+'clinical_notes.json')
  #coll="ACRIN-NSCLC-FDG-PET (ACRIN 6668)"
  #coll2='QIN-HeadNeck'
  #clinJson={coll: clinJsonF[coll], coll2: clinJsonF[coll2]}
  #clinJson = { coll2: clinJsonF[coll2]}
  collec=list(clinJson.keys())
  collec.sort()
  client = bigquery.Client()
  query = "select tcia_wiki_collection_id, idc_webapp_collection_id from `idc-dev-etl.idc_current.original_collections_metadata`"
  job = client.query(query)

  for row in job.result():
    wiki_collec=row['tcia_wiki_collection_id']
    idc_webapp=row['idc_webapp_collection_id']
    #print(row)
    if wiki_collec in clinJson:
      clinJson[wiki_collec]['idc_webapp'] = idc_webapp

  for colInd in range(len(collec)):
    coll=collec[colInd]
    if 'spec' in clinJson[coll]:
      if clinJson[coll]['spec'] == 'acrin':
        parse_acrin_collection(clinJson,coll)
        pass
    else:
      pass
      parse_conventional_collection(clinJson, coll)




  clin_meta=  CURRENT_VERSION +'_clinical_meta.json'
  clin_summary = CURRENT_VERSION +'_clinical_summary.json'
  export_meta_to_json(clinJson,clin_meta,clin_summary)
  i=1

