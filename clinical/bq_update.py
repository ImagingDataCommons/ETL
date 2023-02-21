from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys

from bq_export import DEFAULT_SUFFIX, DEFAULT_DESCRIPTION, DEFAULT_PROJECT, DICOM_META, CURRENT_VERSION, LAST_VERSION, FINAL_PROJECT, DATASET, LAST_DATASET

from bq_export import load_meta_summary, load_meta, load_clin_files

SRCFILES=["prostatex_findings.json", "prostatex_images.json", "prostatex_ktrans.json"]
UPDATENUM="1"

if __name__=="__main__":
  project = DEFAULT_PROJECT
  dataset = DATASET
  filenm="./" + CURRENT_VERSION + "_" + UPDATENUM + "_table_metadata.json"
  load_meta_summary(project, dataset, [], filenm)

  filenm = "./" + CURRENT_VERSION + "_" + UPDATENUM + "_column_metadata.json"
  load_meta(project, dataset, filenm, [])

  dirnm = "./clin_" + CURRENT_VERSION
  load_clin_files(project, dataset, dirnm, SRCFILES)

