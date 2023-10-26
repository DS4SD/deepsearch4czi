# import standard dependenices

import os
import re

import json
import glob

import tqdm
import copy
import pandas as pd

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

from deepsearch_glm.utils.load_pretrained_models import load_pretrained_nlp_models
from deepsearch_glm.nlp_utils import init_nlp_model, print_on_shell


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
        idir = args.input
        ifiles = glob.glob(os.path.join(args.input, "*.json"))
    else:
        idir = os.path.basedir(args.input)
        ifiles = [args.input]

    return idir, ifiles

def process_docs(idir, ifiles):

    load_pretrained_nlp_models(force=False, verbose=True)
    #mdl = init_nlp_model("link;reference")
    mdl = init_nlp_model("link;reference")

    links=[]
    
    for ifile in tqdm.tqdm(ifiles):

        print(f"analysing {ifile}")
        with open(ifile, "r") as fr:
            doc = json.load(fr)

        enriched_doc = mdl.apply_on_doc(doc)

        insts = enriched_doc["instances"]

        headers = insts["headers"]
        links += insts["data"]

        df = pd.DataFrame(insts["data"], columns=insts["headers"])
        #print(df)
        
        titles = df[(("reference"==df["type"]) & ("title"==df["subtype"]))]
        print(titles)
        
        
    df = pd.DataFrame(links, columns=insts["headers"])
    print(df)

    for i,row in df.iterrows():
        if re.match("https://github.com/.+", row["name"]):
            print(row["name"])
    
        
if __name__=="__main__":

    idir, ifiles = parse_arguments()

    print(f"pdf-dir: {idir}")
    print(f"pdf-files: {ifiles}")
    
    process_docs(idir, ifiles)
