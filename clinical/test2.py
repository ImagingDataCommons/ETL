from docx import Document
from docx2python import docx2python
import itertools
import re
if __name__=="__main__":
  document = Document("/Users/george/fed/actcianable/output/clinical_files/PROSTATEx/ProstateX-TrainingLesionInformationv2/ProstateX-DataInfo-Train.docx");
  document2 = docx2python("/Users/george/fed/actcianable/output/clinical_files/PROSTATEx/ProstateX-TrainingLesionInformationv2/ProstateX-DataInfo-Train.docx");
  flt=document2.document[0][0][0]
  col=""
  val=""
  for nxtStr in flt:
    if re.search("^--\\t",nxtStr):
      nxtStr=nxtStr.replace("--\t","")
      print(nxtStr)
  kk=1