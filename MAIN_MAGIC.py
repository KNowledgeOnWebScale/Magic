from MAGIC import Magic
from rdflib_hdt import HDTStore
from ink.base.connectors import AbstractConnector
from rdflib import Graph
import awena
import glob
import pandas as pd
from tqdm import tqdm
import json

skip_list_wiki = ['http://schema.org/', 'http://www.w3.org/2004/02/skos/core', 'http://www.wikidata.org/prop/P',
            'http://www.wikidata.org/prop/direct-normalized/P','http://www.wikidata.org/entity/statement/']

global g

class WikiMagic(Magic):
    def __init__(self, connector, structured_file, header, index_col, main_col):
        super().__init__(connector, structured_file, header, index_col, main_col,'http://www.wikidata.org/prop/direct/','http://www.wikidata.org/prop/direct/P31§', skip_list_wiki)
        self.crawler = awena.Crawler('en', connector)

    def search_entity_api(self, entity):
        try:
            entity = entity.split(',')[0]
            data = ['http://www.wikidata.org/entity/' + x for x in self.crawler.search(entity)]
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
    store = HDTStore("/users/bsteenwi/dbpedia_hdt/wikidata20210305.hdt")
    g = Graph(store)
    connector = HDTConnector()

    #cpa_targets = pd.read_csv("HardTablesR3_CPA_WD_Round3_Targets.csv", header=None)
    cta_targets = pd.read_csv("CTA_WD_Round1_Targets.csv", header=None)

    for file in tqdm(glob.glob('/users/bsteenwi/dbpedia_hdt/code/WikidataR1/*.csv')):
        name = file.split('/')[-1].split('.')[0]
        # cpa_targets[cpa_targets[0] == name][1].value_counts().idxmax()
        for main_col in cta_targets[cta_targets[0]==name][1].values:
            annotator = WikiMagic(connector,file,0,None,main_col)#WikiMagic(connector,file,main_col)
            annotator.annotate()
            annotator.export_files("WikidataR1")



