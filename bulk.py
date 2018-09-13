
from elasticsearch.helpers import bulk
from elasticsearch.helpers import parallel_bulk
from elasticsearch.client import IndicesClient
from apps.busca_textual_es.elasticsearch.utils import chunker
from config import client_es
import os
import asyncio


class IndexBase:

    def __init__(self, **kwargs):
        self.index = kwargs.pop('index')
        self.doc_type = kwargs.pop('doc_type')
        self.kwargs = kwargs
        self.client = client_es
        self.client_index = IndicesClient(self.client)
        self.items = kwargs.pop('items')

        if self.exist_index():
            self.delete_index()
            self.create_index()
        else:
            self.create_index()

    def exist_index(self):
        return self.client_index.exists(index=self.index)

    def delete_index(self):
        return self.client_index.delete(index=self.index, ignore=[400, 404])

    def create_index(self):
        doc = {
              "settings": {
                "analysis": {
                  "analyzer": {
                    "folding": {
                      "tokenizer": "standard",
                      "filter":  ["lowercase", "asciifolding"]
                    }}}}}
        return self.client_index.create(index=self.index, body=doc)


class IndexBulk(IndexBase):

    def create_bulk(self, itens):

        records = []
        for item in itens:
            if item.get('_id'):
                item.pop('_id')

            yield {
                "_index": self.index,
                "_type": self.doc_type,
                "_source": item
            }

    async def load_bulk(self, items_chunk):
        payload = self.create_bulk(items_chunk)
        try:
            success, _ = parallel_bulk(
                self.client, payload, chunk_size=1000)
        except:
            pass

    async def main(self):
        tasks = []
        items = self.items
        print('creating async tasks')
        for items_chunk in chunker(items, 1000):
            tasks.append(asyncio.ensure_future(
                self.load_bulk(items_chunk)
                )
            )
        print('sending to ES')
        await asyncio.gather(*tasks)
        items.close()
