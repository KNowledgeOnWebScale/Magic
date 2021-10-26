# Magic: Mining an Augmented Graph using INK, starting from a CSV

Magic is a tool to transform CSV files into semanticly annotated, structured data while also being able to further augment the dataset based on these semantic annotations.

Magic is higly dependent upon INK, an approach which can generate intepretable embeddings.<br>
More information about INK can be found at: https://github.com/IBCNServices/INK

Magic also uses a HDT-based triple store to extract semantic information.<br>
More information about HDT can be found at: https://www.rdfhdt.org

Three main files are provideed in this repository:
- MAGIC.py: defines the general Magic class to perform semantic annotations given a csv file.
- MAIN_MAGIC.py: shows an example how magic can be used to derive semantic annotations from within the Wikidata knowledge graph
- MAIN_MAGIC_DB.py: shows an example how magic can be used to derive semantic annotations from within the DBpedia knowledge graph

To search for possible candidates matches, magic uses either:
- DBPedia Spotlight when DBPedia annotaions are requested (https://github.com/dbpedia-spotlight/dbpedia-spotlight)
- The Wikidata api (The awena.py script, which is an adapted version of https://github.com/sedthh/awena-wikidata-crawler)

## Demo

To run demo application, pip install all packages inside the `requirements.txt` file <br>
You also need the dbpedia-2016-10 hdt files to build the candidates neighbourhood. You can download it either from the HDT website or from https://www.kaggle.com/bsteenwi/dbpedia. 
<br> Place the hdt and index file in the same folder as the StreamlitApp.py file.

Next, execute the following command inside a terminal window:

```
streamlit run StreamlitApp.py
```

A video of this demo application is also made available (here)[https://www.youtube.com/watch?v=ZhTKxcTBZNE]

## How to cite:
Comming soon, Magic is being used at the ISWC2021 Tabular Data to Knowledge Graph Matching" competition.<br>
A paper describing the full system will be made available.
