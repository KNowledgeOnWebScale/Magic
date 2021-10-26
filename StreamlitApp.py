import streamlit as st
from stqdm import stqdm
import pandas as pd

from StreamlitMAGIC import Magic
from rdflib_hdt import HDTStore
from ink.base.connectors import AbstractConnector
from rdflib import Graph
import awena
import glob
import pandas as pd
from tqdm import tqdm
import json
import requests
import operator

store = HDTStore("dbpedia2016-10.hdt")
g = Graph(store)

skip_list_db = ['http://schema.org/', 'http://www.w3.org/2004/02/skos/core', 'http://www.w3.org/2002/07/owl#sameAs',
                'http://purl.org/dc/terms/subject', 'http://www.w3.org/2002/07/owl#Thing']

class DBMagic(Magic):
    def __init__(self, connector, structured_file, header, index_col, main_col):
        super().__init__(connector, structured_file, header, index_col, main_col,'http://dbpedia.org/','http://www.w3.org/1999/02/22-rdf-syntax-ns#type§', skip_list_db)

    def search_entity_api(self, entity):
        try:
            data = {'confidence': 0, 'text': '"'+entity+'"'}
            headers = {
                'Accept': 'application/json',
            }
            response = requests.post('https://api.dbpedia-spotlight.org/pt/candidates', headers=headers, data=data)
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

def main():
    connector = HDTConnector()
    true_b=False
    true_c=False
    true_d=False
    true_e=False
    true_f=False

    def generate_dataframe(df):
        st.session_state.frame = df
        st.write(df)
        return True

    st.markdown("# Magic: Demonstrator")

    if 'frame' not in st.session_state:
        spectra = st.file_uploader("",type={"csv", "txt"})

        col1, col2, col3 = st.columns(3)

        if col2.button('Example'):
            data=[["The Killers","Brandon Flowers", "1981-06-21", "Singer"],
            ["Coldplay", "Chris Martin","1977-03-02","Singer"],
            ["U2","The Edge","1961-08-08","Guitarist"],
            ["ABBA","Benny Andersson","1946-12-16","Composer"]]
            example = pd.DataFrame(data)
            my_expander = st.expander("Original data", expanded=True)
            with my_expander:
                true_b=generate_dataframe(example)


        if spectra is not None:
            spectra_df = pd.read_csv(spectra)
            my_expander = st.expander("Original data", expanded=True)
            with my_expander:
                true_b=generate_dataframe(spectra_df)

    else:
        spectra = st.file_uploader("upload file", type={"csv", "txt"})

        col1, col2, col3 = st.columns(3)

        if col2.button('Example'):
            data=[["The Killers","Brandon Flowers", "1981-06-21", "Singer"],
            ["Coldplay", "Chris Martin","1977-03-02","Singer"],
            ["U2","The Edge","1961-08-08","Guitarist"],
            ["ABBA","Benny Andersson","1946-12-16","Composer"]]
            example = pd.DataFrame(data)
            true_b=generate_dataframe(example)
        my_expander = st.expander("Original data", expanded=True)
        with my_expander:
            true_b = generate_dataframe(st.session_state.frame)

    my_annotator = st.expander("Annotate data:", expanded=True)
    with my_annotator:
        if true_b:
                if 'option' not in st.session_state:
                    st.session_state.option = st.selectbox('Select major column',tuple(st.session_state.frame.columns))
                    st.write('You selected major column:', st.session_state.option)
                    true_c = True
                else:
                    st.session_state.option = st.selectbox('Select major column',tuple(st.session_state.frame.columns))
                    st.write('You selected major column:', st.session_state.option)
                    true_c = True

        if true_c:
            if 'annotator' in st.session_state:
                true_d = True
                if st.button('Start annotation'):
                    st.session_state.frame.to_csv('tmp.csv', index=None)
                    annotator = DBMagic(connector,"tmp.csv",0,None,st.session_state.option)#WikiMagic(connector,file,main_col)
                    annotator.annotate()
                    annotated_frame = st.session_state.frame.copy()
                    for key in annotator.cea:
                        annotated_frame.at[key[0],key[1]] = '<a target="_blank" href="{}" title="{}">{}</a>'.format(annotator.cea[key],annotator.cea[key], annotated_frame.at[key[0],key[1]])

                    cols = list(annotated_frame.columns)
                    for val in annotator.cta:
                        cols[val[1]] = '<a target="_blank" href="{}" title="{}">{}</a>'.format(val[2],val[2], cols[val[1]])

                    annotated_frame.columns = cols
                    st.session_state.annotator = annotator
                    true_d = True
            else:
                if st.button('Start annotation'):
                    st.session_state.frame.to_csv('tmp.csv', index=None)
                    annotator = DBMagic(connector,"tmp.csv",0,None,st.session_state.option)#WikiMagic(connector,file,main_col)
                    annotator.annotate()
                    st.session_state.annotator = annotator
                    true_d = True

    my_results = st.expander("Results: ", expanded=True)
    with my_results:
        if true_d:
            annotated_frame = st.session_state.frame.copy()
            st.session_state.inv_dct = {}
            for key in st.session_state.annotator.cea:
                st.session_state.inv_dct[st.session_state.annotator.cea[key]] = annotated_frame.at[key[0],key[1]]
                annotated_frame.at[key[0],key[1]] = '<a target="_blank" href="{}" title="{}">{}</a>'.format(st.session_state.annotator.cea[key],st.session_state.annotator.cea[key], annotated_frame.at[key[0],key[1]])
            cols = list(annotated_frame.columns)
            st.session_state.col_to_extend = []
            for val in st.session_state.annotator.cta:
                st.session_state.col_to_extend.append(cols[val[1]])
                cols[val[1]] = '<a target="_blank" href="{}" title="{}">{}</a>'.format(val[2],val[2], cols[val[1]])

            annotated_frame.columns = cols

            st.write(annotated_frame.to_html(escape=False), unsafe_allow_html=True)
            st.write(" ")
            for val in st.session_state.annotator.cpa:
                best_rel = max(st.session_state.annotator.cpa[val].items(), key=operator.itemgetter(1))[0]
                st.write("relation between col "+str(val[0]) +" and col "+str(val[1]) +" is: "+best_rel)

    my_augment = st.expander("Augment data:", expanded=True)
    with my_augment:
        if 'col_to_extend' in st.session_state:
            if 'option_augment' not in st.session_state:
                st.session_state.option_augment = st.selectbox('Select augmentation column',st.session_state.col_to_extend)
                st.write('You selected:', st.session_state.option)
                true_e = True
            else:
                st.session_state.option_augment = st.selectbox('Select augmentation column',st.session_state.col_to_extend)
                st.write('You selected:', st.session_state.option_augment)
                true_e = True
            if true_e:
                if 'aug_cols' not in st.session_state:
                    if st.button('Start Augmentation'):
                        aug_data = st.session_state.annotator.augement(st.session_state.option_augment)
                        frame = st.session_state.frame.copy()
                        for key in st.session_state.annotator.cea:
                            frame.at[key[0],key[1]] = st.session_state.annotator.cea[key]
                        orig_cols = list(frame.columns)
                        for key in aug_data:
                            frame = pd.merge(frame,aug_data[key], left_on=st.session_state.option_augment, right_index=True)

                        for key in st.session_state.annotator.cea:
                            frame.at[key[0],key[1]] = st.session_state.inv_dct[st.session_state.annotator.cea[key]]
                        st.session_state.aug_frame = frame
                        st.session_state.aug_cols = orig_cols
                        true_f = True
                else:
                    if st.button('Start Augmentation'):
                        aug_data = st.session_state.annotator.augement(st.session_state.option_augment)
                        frame = st.session_state.frame.copy()
                        for key in st.session_state.annotator.cea:
                            frame.at[key[0],key[1]] = st.session_state.annotator.cea[key]
                        orig_cols = list(frame.columns)
                        for key in aug_data:
                            frame = pd.merge(frame,aug_data[key], left_on=st.session_state.option_augment, right_index=True)

                        for key in st.session_state.annotator.cea:
                            frame.at[key[0],key[1]] = st.session_state.inv_dct[st.session_state.annotator.cea[key]]
                        st.session_state.aug_frame = frame
                        st.session_state.aug_cols = orig_cols
                    true_f = True

            if true_f:
                print(st.session_state.aug_cols)
                cols = st.multiselect('select columns:', st.session_state.aug_frame.columns, default=st.session_state.aug_cols)
                print(cols)
                # show dataframe with the selected columns
                st.write(st.session_state.aug_frame[cols])
                st.session_state.aug_cols = cols


if __name__ == '__main__':
    main()
