from abc import ABC, abstractmethod
from giraffe.data_access.formats.progress_report_format import ProgressReport
from giraffe.data_access.formats.status_report_format import StatusReport


# A report handler may write report to different targets
# E.g.: Relational Database, Elasticsearch, Redis, ....


class ReportHandler(ABC):

    @abstractmethod
    def progress_report(self, progress_report: ProgressReport) -> None:
        pass

    @abstractmethod
    def status_report(self, status_report: StatusReport) -> None:
        pass

    @abstractmethod
    def create_structures_if_not_exists(self) -> bool:
        # For example: create tables needed tor status/progress reports in the DB
        pass
