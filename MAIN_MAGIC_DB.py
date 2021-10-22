from MAGIC import Magic
from rdflib_hdt import HDTStore
from ink.base.connectors import AbstractConnector
from rdflib import Graph
import awena
import glob
import pandas as pd
from tqdm import tqdm
import json
import requests

skip_list_db = ['http://schema.org/', 'http://www.w3.org/2004/02/skos/core', 'http://www.w3.org/2002/07/owl#sameAs',
            'http://dbpedia.org/property/wikiPageUsesTemplate','http://dbpedia.org/ontology/wikiPageWikiLink',
                'http://purl.org/dc/terms/subject', 'http://www.w3.org/2002/07/owl#Thing']

global g

class DBMagic(Magic):
    def __init__(self, connector, structured_file, header, index_col, main_col):
        super().__init__(connector, structured_file, header, index_col, main_col,'http://dbpedia.org/property/','http://www.w3.org/1999/02/22-rdf-syntax-ns#type§', skip_list_db)

    def search_entity_api(self, entity):
        try:
            data = {'confidence': 0.1, 'text': entity}
            headers = {
                'Accept': 'application/json',
            }
            response = requests.post('http://localhost:2222/rest/candidates', headers=headers, data=data)
            js = json.loads(response.text)

            if isinstance(js['annotation']['surfaceForm'],list):
                data = []
                for s in js['annotation']['surfaceForm']:
                    if isinstance(s['resource'], list):
                        data.extend(['http://dbpedia.org/resource/'+r['@uri'] for r in s['resource']])
                    else:
                        data.extend(['http://dbpedia.org/resource/'+s['resource']['@uri']])
            else:
                if isinstance(js['annotation']['surfaceForm']['resource'], list):
                    data = ['http://dbpedia.org/resource/' + r['@uri'] for r in js['annotation']['surfaceForm']['resource']]
                else:
                    data = ['http://dbpedia.org/resource/' + js['annotation']['surfaceForm']['resource']['@uri']]
        except:
            data = []
        return data


class HDTConnector(AbstractConnector):
    def __init__(self):
        self.db_type = 'rdflib'

    def query(self, q_str):
        res = g.query(q_str)
        return json.loads(res.serialize(format="json"))['results']['bindings']

    def query_relation(self, ind, rel):
        q_str="""
        SELECT ?o ?l WHERE {
            <"""+str(ind)+"> <"+str(rel)+"> ?o"+""".
            ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l .
            FILTER (langMatches( lang(?l), "EN" ) )
        }
        """
        res = g.query(q_str)
        return json.loads(res.serialize(format="json"))['results']['bindings']

    def query_column(self, rel):
        q_str="""
        SELECT ?s ?o ?l WHERE {
            ?s"""+" <"+str(rel)+"> ?o"+""".
        }
        ORDER BY RAND() 
        LIMIT 250
        """
        res = g.query(q_str)
        return json.loads(res.serialize(format="json"))['results']['bindings']

if __name__ == '__main__':
    store = HDTStore("/users/bsteenwi/dbpedia_hdt/dbpedia2016-10.hdt")
    g = Graph(store)
    connector = HDTConnector()
    #cpa_targets = pd.read_csv("HardTablesR3_CPA_WD_Round3_Targets.csv", header=None)
    cta_targets = pd.read_csv("CTA_DBP_Round1_Targets.csv", header=None)
    done = pd.read_csv("DBPediaR1_DB_cta.txt", header=None)
    done = set(done[0].values)
    for file in tqdm(glob.glob('/users/bsteenwi/dbpedia_hdt/code/DBPediaR1/*.csv')):
        name = file.split('/')[-1].split('.')[0]
        print(name)
        if name not in done:
            # cpa_targets[cpa_targets[0] == name][1].value_counts().idxmax()

            for main_col in cta_targets[cta_targets[0]==name][1].values:
                annotator = DBMagic(connector,file,0,None,main_col)#WikiMagic(connector,file,main_col)
                annotator.annotate()
                annotator.export_files("DBPediaR1_DB")



