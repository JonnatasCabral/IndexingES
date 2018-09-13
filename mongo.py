from pymongo import MongoClient
from apps.busca_textual_es.elasticsearch.mixins import RunnerMixin
from config import mongo
import os


class IndexItemsFromMongo(RunnerMixin):
        
    def get_items(self):
        collection_name = self.kwargs.get('collection_name')
        print('-> começando execução <-')
        db = mongo
        collection = db[collection_name]

        pipeline = [
            {
                '$unwind': '$andamentos'
            },
            {
                '$project': {
                    'titulo': '$andamentos.titulo',
                    'texto': '$andamentos.texto', 'data': '$andamentos.data',
                    'extra': '$andamentos.extra', 'numero': 1,
                    '_id': 0
                }
            }
        ]

        andamentos_query = collection.aggregate(pipeline)

        return andamentos_query

    
