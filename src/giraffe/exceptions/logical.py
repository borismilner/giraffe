from giraffe.exceptions.giraffe_exception import GiraffeException


class QuerySyntaxError(GiraffeException):
    pass


class MissingKeyError(GiraffeException):
    pass


class UnexpectedOperation(GiraffeException):
    pass


class PropertyNotIndexedError(GiraffeException):
    pass


class ComputeDataFrameFailure(GiraffeException):
    pass


class NoSuchFileError(GiraffeException):
    pass


class UnexpectedResult(GiraffeException):
    pass


class TranslationError(GiraffeException):
    pass


class ParseModelFailure(GiraffeException):
    pass


class TransformerError(GiraffeException):
    pass
