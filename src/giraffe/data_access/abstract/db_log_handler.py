from abc import ABC, abstractmethod


# A report handler may write report to different targets
# E.g.: Relational Database, Elasticsearch, Redis, ....

class DbLogHandler(ABC):

    @abstractmethod
    def add_entry(self, entry) -> None:
        pass

    @abstractmethod
    def create_structures_if_not_exists(self) -> bool:
        # For example: create tables needed tor status/progress reports in the DB
        pass
