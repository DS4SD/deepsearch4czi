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

import matplotlib.pyplot as plt

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

def annotate_doc(ifile):

    if ifile.endswith(".nlp.json"):
        return
        
    mdl = init_nlp_model("link")
    
    with open(ifile, "r") as fr:
        doc = json.load(fr)

    enriched_doc = mdl.apply_on_doc(doc)
        
    ofile = ifile.replace(".json", ".nlp.json")
    with open(ofile, "w") as fw:
        fw.write(json.dumps(enriched_doc))                    

def process_all_docs(idir, ifiles):

    processed=[]
    tasks=[]

    sfiles = set(ifiles)
    
    for ifile in ifiles:

        if ifile.endswith(".nlp.json"):
            processed.append(ifile)
            continue
        else:
            ofile = ifile.replace(".json", ".nlp.json")

            if ofile not in sfiles:
                tasks.append(ifile)
                processed.append(ofile)
                
    if len(tasks)==0:
        return processed

    print("#-tasks: ", len(tasks))
    
    with ProcessPoolExecutor(max_workers=12) as executor:
        results = executor.map(annotate_doc, tasks)

    return processed
        
def extract_links(idir, ifiles):

    ofile = os.path.join(idir, "links.csv")
    print("dataframe: ", ofile)


    if os.path.exists(ofile):
        df = pd.read_csv(ofile)
        return df


    
    links=[]
    
    for ifile in tqdm.tqdm(ifiles):

        try:
            with open(ifile, "r") as fr:
                enriched_doc = json.load(fr)
        except:
            continue
        
        insts = enriched_doc["instances"]
                
        headers = insts["headers"]

        for row in insts["data"]:
            name = row[headers.index("name")]

            for _ in [",", "/", "/issues"]:
                if name.endswith(_):
                    name = name[:-len(_)]

            row[headers.index("name")] = name
                    
            links.append(row)
        
        """
        df = pd.DataFrame(insts["data"], columns=insts["headers"])
        #print(df)
        
        titles = df[(("reference"==df["type"]) & ("title"==df["subtype"]))]
        print(titles)
        """
        
    df = pd.DataFrame(links, columns=insts["headers"])
    df.to_csv(ofile)

    print(df)

    return df

def extract_github(df):

    print(df)
    
    result = df['name'].str.contains("https://github.com/.+", regex=True)
    github = df[result]
    print(github)

    hist = github["name"].value_counts()
    print(hist)

    table=[]
    
    x=[]
    y=[]
    l=[]
    i=1
    for key,val in hist.items():
        print(key, "\t", val)
        x.append(i)
        l.append(key)
        y.append(val)

        i += 1

        table.append([val, key])
        
        if i>20:
            break

    print("\n\n bare results: \n")
        
    print(tabulate(table, headers=["count", "link"]))

    print("\n\n")
    
    fig = plt.figure(1)
    plt.semilogy(x,y)
    plt.show()

    
    
if __name__=="__main__":

    idir, ifiles = parse_arguments()

    print(f"pdf-dir: {idir}")
    print(f"#-files: {len(ifiles)}")
    
    ofiles = process_all_docs(idir, ifiles)
    
    df = extract_links(idir, ofiles)

    extract_github(df)
