# import standard dependenices
import os
import json
import glob

import tqdm
import copy
#import pandas as pd

import argparse
import subprocess

from tabulate import tabulate

from concurrent.futures import ProcessPoolExecutor

# Import the deepsearch-toolkit
import deepsearch as ds
from deepsearch.cps.client.api import CpsApi
from deepsearch.cps.client.components.elastic import ElasticDataCollectionSource
from deepsearch.cps.queries import DataQuery, DocumentQuestionQuery
from deepsearch.cps.client.components.queries import RunQueryError

from deepsearch.documents.core.models import ConversionSettings, DefaultConversionModel, ProjectConversionModel, \
    OCRSettings

def parse_arguments():

    parser = argparse.ArgumentParser(
        prog = 'extract_software',
        description = 'Extract software references from PDF documents using `Deep Search` ',
        epilog = '')

    parser.add_argument('-i', '--input', required=True,
                        help="input (singl PDF files or directory of PDF files)",
                        default="./data")    

    args = parser.parse_args()

    if not os.path.exists(args.input):
        exit(-1)
    elif os.path.isdir(args.input):
        pdf_dir = args.input
        pdf_files = glob.glob(os.path.join(args.input, "*.pdf"))
    else:
        pdf_dir = os.path.basedir(args.input)
        pdf_files = [args.input]

    return pdf_dir, pdf_files

def process_zip_files(tdir):

    zipfiles = sorted(glob.glob(os.path.join(tdir, "*.zip")))
    #print(f"zips: ", len(zipfiles))

    if len(zipfiles)==0:
        return 
    
    for zipfile in zipfiles:
        cmd = ["unzip", zipfile, "-d", tdir]
        print(" ".join(cmd))
        
        subprocess.call(cmd)

    # clean up
    for i,zipfile in enumerate(zipfiles):
        print(i, "\t removing ", zipfile)
        subprocess.call(["rm", zipfile])

def convert_pdfs(pdf_dir, pdf_files):

    PROJ_KEY="1234567890abcdefghijklmnopqrstvwyz123456"
    
    api = CpsApi.from_env(profile_name="ds-experience")

    ## Modify conversion pipeline
    cs = ConversionSettings.from_project(api, proj_key=PROJ_KEY)

    # OCR
    cs.ocr.enabled = False ## Enable or disable OCR
    # cs.ocr.merge_mode = "prioritize-ocr" # Pick how OCR cells are treated when mixed with programmatic content

    # backends = OCRSettings.get_backends(api) # list OCR backends
    #cs.ocr.backend = "alpine-ocr" ## Pick OCR backend
    
    for pdf_file in pdf_files:

        json_file = pdf_file.replace(".pdf", ".json")

        if os.path.exists(json_file): # already converted
            continue
        
        documents = ds.convert_documents(
            api=api,
            proj_key=PROJ_KEY,
            source_path=pdf_file,
            conversion_settings=cs,
            progress_bar=True
        )           
        documents.download_all(result_dir=pdf_dir)
        
        info = documents.generate_report(result_dir=pdf_dir)
        print(info) 

    process_zip_files(pdf_dir)

def process_docs(pdf_dir):

    fdocs = sorted(glob.glob(os.path.join(tdir, "*.json")))    

    for fdoc in fdocs:

        with open(fdoc, "r") as fr:
            doc = json.load(fr)
    
if __name__=="__main__":

    pdf_dir, pdf_files = parse_arguments()

    print(f"pdf-dir: {pdf_dir}")
    print(f"pdf-files: {pdf_files}")
    
    convert_pdfs(pdf_dir, pdf_files)
