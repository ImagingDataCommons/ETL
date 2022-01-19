import json
import re
import pandas as pd
import numpy as np
#import xlrd
#from openpyxl import Workbook, load_workbook
from os import path

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

#def analyzeDataFrame(df, ptId):



def processSrc(fpath, colName, srcInfo):
  attrs=[]
  filenm = fpath+colName+'/'+srcInfo['filenm']
  sheetNo = (0 if not 'sheet' in srcInfo else srcInfo['sheet'])
  patientIdRow = (0 if not ('patientIdRow') in srcInfo else srcInfo['patientIdRow'])
  cols = ([0] if not 'headcols' in srcInfo else srcInfo['headcols'])
  skip = (None if not 'skipRows' in srcInfo else srcInfo['skipRows'])
  pivot= (False if not 'pivot' in srcInfo else srcInfo['pivot'])
  extension = path.splitext(filenm)[1]
  engine='xlrd'
  if extension == '.xlsx':
    engine= 'openpyxl'
  elif extension == '.xlsb':
    engine = 'pyxlsb'
  df=[]
  if extension == '.csv':
    df = pd.read_csv(filenm)
  else:
    df = pd.read_excel(filenm, engine=engine, sheet_name=sheetNo)
  if pivot:
    df = df.T
    cols =[cols[i]+1 for i in range(len(cols))]
    colList=list(df.columns)
    df.insert(0,'tmp',list(df.index))

  for i in range(len(df.columns)):
    attrs.append([])

  for i in range(len(cols)):
    colVal=''
    ind = cols[i]
    if ind == 0:
      values = df.columns
    else:
      values=df.values[ind-1]
    for j in range(len(values)):
      val=values[j]
      if (i == len(cols)-1) or (not (str(val) == 'nan') and not ('Unnamed:' in str(colVal))):
        colVal=val
      if (i < (len(cols)-1)) or (not (str(colVal) == 'nan') and not ('Unnamed:' in str(colVal))):
        attrs[j].append(colVal)

  drcols=[i-1 for i in cols]
  if skip is not None:
    skip = [skip[i]-1 for i in range(len(skip))]
    drcols =drcols+skip

  if -1 in drcols:
    drcols.remove(-1)
  df.drop(df.index[drcols], inplace=True)

  i=1
  headers = renameHeader(attrs)
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
            kk=1

    df = pd.concat([df_new])

  try:
    df[df.columns[patientIdRow]] = df[df.columns[patientIdRow]].astype('Int64')
  except:
    df[df.columns[patientIdRow]] = df[df.columns[patientIdRow]].astype('str')
  return [headerSet,df]

def renameHeader(attrs):
  headcols=[]
  for i in range(len(attrs)):
    headSet=attrs[i]
    header='_'.join(str(k) for k in headSet)
    header=header.replace('/','_')
    header=header.replace('-', '_')
    header=header.replace(' ', '_')
    headcols.append(header)
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

  k=1
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

    #join data frames

    new_df = clinJson[coll]['cols'][attrSetInd][batchSetInd]['df']

    #make sure joining df is using the same patientId column name as the original
    colList=list(new_df.columns)
    colList[ptRowIds[batchSetInd]] = ptRow
    new_df.columns = colList
    df_all_rows=pd.concat([df_all_rows,new_df])
  clinJson[coll]['mergeBatch'][attrSetInd]['cList'] = cList
  clinJson[coll]['mergeBatch'][attrSetInd]['df'] = df_all_rows


if __name__=="__main__":
  clinJson =read_clin_file('/Users/george/fed/actcianable/output/clinical_notes.json')
  i=1
  collec=list(clinJson.keys())
  collec.sort()
  i=1
  #attrs = {}
  for colInd in range(len(collec)):
    coll=collec[colInd]
    #attrs[coll]={}
    if 'srcs' in clinJson[coll]:
      clinJson[coll]['cols']=[]
      for attrSetInd in range(len(clinJson[coll]['srcs'])):
        ptRowIds=[]
        cohortSeries=clinJson[coll]['srcs'][attrSetInd]
        clinJson[coll]['cols'].append([])
        wJson = False
        for batchSetInd in range(len(clinJson[coll]['srcs'][attrSetInd])):
          clinJson[coll]['cols'][attrSetInd].append([])
          clinJson[coll]['cols'][attrSetInd][batchSetInd] = {}
          srcInfo = clinJson[coll]['srcs'][attrSetInd][batchSetInd]
          patientIdRow = (0 if not ('patientIdRow') in srcInfo else srcInfo['patientIdRow'])
          ptRowIds.append(patientIdRow)
          print("strcInfo "+ str(srcInfo))
          if not ('type' in srcInfo) or not (srcInfo['type'] == 'json'):
            [headers,df] = processSrc('/Users/george/fed/actcianable/output/clinical_files/',coll,srcInfo)
            #attrs.append([attr])
            clinJson[coll]['cols'][attrSetInd][batchSetInd]['headers'] = headers
            clinJson[coll]['cols'][attrSetInd][batchSetInd]['df'] = df
          else:
            wJson = True
        if not wJson:
          mergeAcrossBatch(clinJson,coll,ptRowIds,attrSetInd)
      if not wJson:
        mergeAcrossAttr(clinJson,coll)
  #write_clin_file('./clinical_out.json',clinJson)
  i=1

