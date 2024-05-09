from elasticsearch import Elasticsearch
from warnings import filterwarnings
filterwarnings("ignore", ".*")

es = Elasticsearch(["https://localhost:9200"], basic_auth=("elastic", "elastic123"), verify_certs=False)

# es.indices.create(index="person")

# es.index(index="person", id="gsemin", document={
#     "name": "German",
#     "last_name": "Semin",
#     "age": 20,
#     "country": "Czech Republic"
# })

# es.index(index="person", id="gsemin", document={
#     "name": "Jan",
#     "last_name": "Novak",
#     "age": 20,
#     "country": "Czech Republic"
# })

# print(es.search(index="person", size=100)['hits']['hits'])

# es.delete(index="person", id="gsemin")

es.indices.delete(index="person")
