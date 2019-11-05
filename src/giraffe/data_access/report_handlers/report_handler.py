from abc import ABC, abstractmethod

# A report handler may write report to different targets
# E.g.: Relational Database, Elasticsearch, Redis, ....
import giraffe


class ReportHandler(ABC):

    @abstractmethod
    def progress_report(self, pr) -> None:
        pass

    @abstractmethod
    def status_report(self, status_report) -> None:
        pass

    @abstractmethod
    def create_structures_if_not_exists(self) -> bool:
        # For example: create tables needed tor status/progress reports in the DB
        pass
