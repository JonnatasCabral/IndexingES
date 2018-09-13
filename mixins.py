from apps.busca_textual_es.elasticsearch.bulk import IndexBulk
from abc import ABC, abstractmethod
import asyncio


class RunnerMixin(ABC):

    index = None
    doc_type = None

    def __init__(self, index, doc_type, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.index =  index
        self.doc_type = doc_type

    @abstractmethod
    def get_items(self):
        """
            This method must return a generator of dicts
        """
        pass

    def run(self):
        loop = asyncio.get_event_loop()
        loader = IndexBulk(index=self.index,
                           doc_type=self.doc_type,
                           items=self.get_items())

        if asyncio.get_event_loop().is_closed():
            loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(loader.main())
        finally:
            loop.close()
