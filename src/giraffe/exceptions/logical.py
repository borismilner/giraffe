from giraffe.exceptions.giraffe_exception import GiraffeException


class QuerySyntaxError(GiraffeException):
    pass


class MissingKeyError(GiraffeException):
    pass


class UnexpectedOperation(GiraffeException):
    pass


class PropertyNotIndexedError(GiraffeException):
    pass
