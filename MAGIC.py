from __future__ import division
from ink.base.structure import InkExtractor
import pandas as pd
import json
import numpy as np
import operator
from tqdm import tqdm
global g
import re


class Magic(object):
    def __init__(self, connector, structured_file, header, index_col, main_col, property_prefix, cta_filter=None, skiplist=[]):
        self.connector=connector
        self.file = structured_file
        self.name = structured_file.split('/')[-1].split('.')[0]
        self.maincol = main_col
        self.property_prefix=property_prefix
        self.skiplist=skiplist
        self.cea = {}
        self.cpa = {}
        self.cta = set()
        self.cta_filter=cta_filter
        self.header = header
        self.index_col = index_col


    def generate_embedding(self, data, depth, jobs, filter=None):
        extractor = InkExtractor(self.connector, verbose=False)
        X_train, _ = extractor.create_dataset(depth, set(data), set(), self.skiplist, jobs)
        extracted_data = extractor.fit_transform(X_train, counts=False, levels=False, float_rpr=False)
        df_data = pd.DataFrame.sparse.from_spmatrix(extracted_data[0])
        df_data.index = [x for x in extracted_data[1]]
        df_data.columns = extracted_data[2]
        if filter is not None:
            cols = [col for col in df_data.columns if filter in col]
            df_data = df_data[cols]
        return df_data

    def search_entity_api(self, entity):
        raise NotImplementedError("Please Implement this method")

    def annotate(self):
        df = pd.read_csv(self.file, header=self.header, index_col=self.index_col)
        for k, row in tqdm(df.iterrows(), total=len(df)):
            try:
                data = self.search_entity_api(re.sub("[\(\[].*?[\)\]]", "", row[self.maincol]))
            except:
                data = []
            if len(data) > 0:
                emb = self.generate_embedding(data,2,5)
                emb_label = emb[[c for c in emb.columns if self.property_prefix in c]]
                cols = [c for c in emb_label.columns for r in range(len(row)) if
                        row[r] is not np.nan and r != self.maincol and '§' + str(row[r]) in c]
                cols = [c for c in cols if c.count("http") < 2 or 'http://www.w3.org/2000/01/rdf-schema#label§' in c]
                major_ind = emb_label[cols].sum(axis=1).idxmax()
                self.cea[(k, self.maincol)] = major_ind
                major_ind_emb = emb_label.loc[major_ind, cols]
                major_ind_emb = major_ind_emb[(major_ind_emb != 0)]
                major_ind_emb_cols = major_ind_emb.index
                for r in range(len(row)):
                    if r != self.maincol and row[r] is not np.nan:
                        pos_cols = [c for c in cols if str(row[r]) in c]
                        if len(pos_cols) > 0:
                            pos_cols = max(pos_cols, key=len)
                            cpa_relation = pos_cols.split('.http')[0]
                            if '§' in cpa_relation:
                                cpa_relation = cpa_relation.split('§')[0]
                            if (self.maincol, r) not in self.cpa:
                                self.cpa[(self.maincol, r)] = {}
                            if cpa_relation not in self.cpa[(self.maincol, r)]:
                                self.cpa[(self.maincol, r)][cpa_relation] = 0
                            self.cpa[(self.maincol, r)][cpa_relation] += 1
                            res = self.connector.query_relation(major_ind, cpa_relation)
                            for i in res:
                                if i['l']['value'] == row[r]:
                                    self.cea[(k, r)] = i['o']['value']
                                    break

        for k, row in df.iterrows():
            for c in self.cpa:
                try:
                    best_col = max(self.cpa[c].items(), key=operator.itemgetter(1))[0]
                    res = self.connector.query_relation(self.cea[(k, c[0])], best_col)
                except:
                    res = []
                for i in res:
                    if row[c[1]] is not np.nan and i['l']['value'] == row[c[1]]:
                        self.cea[(k, c[1])] = i['o']['value']
                        break

        column_entity = {}
        for c in self.cea:
            if c[1] not in column_entity:
                column_entity[c[1]] = set()
            column_entity[c[1]].add(self.cea[c])

        for col in column_entity:
            try:
                data = []
                self.cta.add((self.name, col, self.generate_embedding(column_entity[col], 1, 50, self.cta_filter).sum().idxmax().split('§')[-1]))
            except:
                None

        print(self.name)
        print(self.maincol)


    def _export_cea(self, prefix):
        with open(prefix+"_cea.txt", "a") as myfile:
            for c in self.cea:
                myfile.write('"' + self.name + '","' + str(c[0]) + '","' + str(c[1]) + '","')
                myfile.write(self.cea[c])
                myfile.write('"\n')

    def _export_cpa(self, prefix):
        with open(prefix + "_cpa.txt", "a") as myfile:
            for c in self.cpa:
                res = max(self.cpa[c].items(), key=operator.itemgetter(1))[0]
                myfile.write('"' + self.name + '","' + str(c[0]) + '","' + str(c[1]) + '","')
                myfile.write(res)
                myfile.write('"\n')

    def _export_cta(self, prefix):
        with open(prefix + "_cta.txt", "a") as myfile:
            for c in self.cta:
                myfile.write('"' + str(c[0]) + '","' + str(c[1]) + '","' + str(c[2]))
                myfile.write('"\n')

    def export_files(self, prefix):
        self._export_cea(prefix)
        self._export_cta(prefix)
        self._export_cpa(prefix)







