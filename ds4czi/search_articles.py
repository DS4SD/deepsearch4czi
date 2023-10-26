# import standard dependenices
import os
import json

import tqdm
import copy
import pandas as pd

import argparse

from tabulate import tabulate

from concurrent.futures import ProcessPoolExecutor

# Import the deepsearch-toolkit
import deepsearch as ds
from deepsearch.cps.client.api import CpsApi
from deepsearch.cps.client.components.elastic import ElasticDataCollectionSource
from deepsearch.cps.queries import DataQuery, DocumentQuestionQuery
from deepsearch.cps.client.components.queries import RunQueryError

def parse_arguments():

    parser = argparse.ArgumentParser(
        prog = 'search-articles',
        description = 'search articles files from `Deep Search` documents',
        epilog = '')

    parser.add_argument('-i', '--index', required=False, 
                        help="deepsearch index", default="arxiv")

    parser.add_argument('-q', '--query', required=True, 
                        help="query to search for documents")
    
    parser.add_argument('-c', '--chunk-size', required=False,
                        help="elastic query chunk-size",
                        default=100)

    parser.add_argument('-o', '--output-dir', required=True,
                        help="output directory for files")


    args = parser.parse_args()
    
    index = args.index
    query = args.query
    
    chunk = int(args.chunk_size)
    odir = args.output_dir

    if not os.path.exists(odir):
        os.makedirs(odir)

    jodir = os.path.join(odir, f"{index}/json")

    if not os.path.exists(jodir):
        os.makedirs(jodir)

    return index, query, chunk, odir, jodir
    
def get_api():

    api = CpsApi.from_env(profile_name="ds-internal")    
    return api

def list_collections(api, INDEX_KEY, CHUNK_SIZE):
    
    # Fetch list of all data collections
    collections = api.elastic.list()
    collections.sort(key=lambda c: c.name.lower())
    
    # Visualize summary table
    results = [
        {
            "Name": c.name,
            "Type": c.metadata.type,
            "Num entries": c.documents,
            "Date": c.metadata.created.strftime("%Y-%m-%d"),
            "Coords": f"{c.source.elastic_id}/{c.source.index_key}",
            "Index": c.source.index_key
        }
        for c in collections
    ]

    df = pd.DataFrame(results)
    
    data = df[df["Index"]==INDEX_KEY]
    
    if len(data)>0:
        print(data)
        return True
    else:
        print(df)
        return False

def get_total_docs(api, index, query, chunk):

    # Input query
    search_query = query
    data_collection = ElasticDataCollectionSource(elastic_id="default",
                                                  index_key=index)

    # Prepare the data query
    query = DataQuery(
        search_query, # The search query to be executed
        source=[
            "description",
            "_s3_data.json-document",
            "file-info.document-hash",
            "file-info.filename"], # Which fields of documents we want to fetch
        limit=chunk, # The size of each request page
        coordinates=data_collection # The data collection to be queries
    )

    # [Optional] Compute the number of total results matched. This can be used to monitor the pagination progress.
    count_query = copy.deepcopy(query)
    count_query.paginated_task.parameters["limit"] = 0
    count_results = api.queries.run(count_query)
    expected_total = count_results.outputs["data_count"]
    expected_chunks = (expected_total + chunk - 1) // chunk # this is simply a ceiling formula

    print(f"#-docs: {expected_total} with {expected_chunks} chunks")

    return expected_total, expected_chunks, query

def retrieve_doc_via_curl(task):
    
    url = task["url"]
    fname = task["filename"]
    
    cmd = f"curl \"{url}\" -o {fname} -s"
    os.system(cmd)

    try:
        with open(fname, "r") as fr:
            doc = json.load(fr)
        
        # inject description
        doc["description"] = task["description"]
        doc["file-info"] = task["file-info"]
        
        with open(fname, "w") as fw:
            fw.write(json.dumps(doc, indent=2))

        return True

    except:
        #print("ERROR with curl query ....")        
        os.remove(fname)        
        return False

def retrieve_doc_via_query(task):

    #print(row)
    url = task["url"]
    fname = task["filename"]   
    fileinfo = task["file-info"]
    index = task["index"]
    #api = row["api"]
    
    dochash = fileinfo["document-hash"]
    
    # Input query
    search_query = f"file-info.document-hash:{dochash}"
    print("serch-query: ", search_query)
    
    data_collection = ElasticDataCollectionSource(elastic_id="default",
                                                  index_key=index)

    # Prepare the data query
    query = DataQuery(
        search_query, # The search query to be executed
        source=[
            "description",
            "_s3_data.json-document",
            "file-info.document-hash",
            "file-info.filename",
            "main-text", "tables", "figures"], # Which fields of documents we want to fetch
        limit=1, # The size of each request page
        coordinates=data_collection # The data collection to be queries
    )    
    
    try:
        api = get_api()
        
        result = api.queries.run(query)

        out = result.outputs        
        doc = out["data_outputs"][0]["_source"]

        with open(fname, "w") as fw:
            fw.write(json.dumps(doc, indent=2))        

        print(f"written {fname}")            
            
        return True
    
    except:
        print("ERROR with ES query ....")
        return False
    
def retrieve_doc(task):

    #print(task)
    
    if os.path.exists(task["filename"]):
        return True

    if task["url"]!=None and retrieve_doc_via_curl(task):
        return True
    
    if retrieve_doc_via_query(task):
        return True

    return False

def retrieve_doc_in_parallel(task):
    return retrieve_doc(task)

def download_docs(api, index, query, chunk_size, odir, jodir):

    dataset = index
    document = None
    dochash = None
    
    expected_total, expected_chunks, query = get_total_docs(api, index, query, chunk_size)

    total=0

    documents=0
    texts=0
    tables=0

    local_texts=0    
    records=[]
    
    cursor = api.queries.run_paginated_query(query)
    for result_page in tqdm.tqdm(cursor, total=expected_chunks):
        
        tasks=[]
        
        # Iterate through the results of a single page, and add to the total list
        for row in result_page.outputs["data_outputs"]:

            if "_s3_data" in row["_source"] and "json-document" in row["_source"]["_s3_data"]:
                surl = row["_source"]["_s3_data"]["json-document"]["url"]
            else:
                surl = None
                
            dhash = row["_source"]["file-info"]["document-hash"]
            fname = os.path.join(jodir, f"{dhash}.json")

            if os.path.exists(fname):
                continue
            
            tasks.append({
                "url": surl,
                "filename": fname,
                "description": row["_source"]["description"],
                "file-info": row["_source"]["file-info"],
                "index": index,
                #"api": api
            })

        with ProcessPoolExecutor(max_workers=16) as executor:
            results = executor.map(retrieve_doc_in_parallel, tasks)
    
if __name__=="__main__":

    index, query, chunk_size, odir, jodir = parse_arguments()
    
    api = get_api()

    if not list_collections(api, index, chunk_size):
        exit(-1)

    download_docs(api, index, query, chunk_size, odir, jodir)
