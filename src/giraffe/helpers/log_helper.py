import logging
import sys
import threading
from logging.handlers import QueueHandler
from logging.handlers import QueueListener
from multiprocessing import Queue

from fluent import handler as fluent_handler

date_time_format = "%d-%m-%Y %H:%M:%S"
log_row_format = '[%(asctime)s] <%(processName)s,%(threadName)s> -%(module)s- %(levelname)s: %(message)s'
log_row_format = logging.Formatter(fmt=log_row_format, datefmt=date_time_format)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_row_format)

ADMINISTRATIVE_LEVEL = 9
logging.addLevelName(ADMINISTRATIVE_LEVEL, "ADMIN")

structured_log_format = {
        'host': '%(hostname)s',
        'time': '%(asctime)s',
        'function': '%(module)s.%(funcName)s',
        'level': '%(levelname)s',
        'stack_trace': '%(exc_text)s'
}
fluentd_handler = fluent_handler.FluentHandler('zodiac.logs',
                                               host='localhost',
                                               port=2104)
formatter = fluent_handler.FluentRecordFormatter(structured_log_format)
fluentd_handler.setFormatter(formatter)
fluentd_handler.setLevel(ADMINISTRATIVE_LEVEL)
fluentd_logger = logging.getLogger('fluentd_logger')
fluentd_logger.setLevel(ADMINISTRATIVE_LEVEL)
fluentd_logger.addHandler(fluentd_handler)
fluentd_logger.propagate = False


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """Handler for unhandled exceptions that will write to the logs"""
    if issubclass(exc_type, KeyboardInterrupt):
        # call the default excepthook saved at __excepthook__
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger = get_logger(logger_name='Unhandled-Exception-Catcher')
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))
    # noinspection PyBroadException
    try:
        pass
        # queue_listener.stop()
    except:
        pass
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


def admin(self, msg, *args, **kwargs):
    global fluentd_logger
    fluentd_logger._log(ADMINISTRATIVE_LEVEL, msg, args, **kwargs)


logging.Logger.admin = admin

logging_queue = Queue(-1)

queue_handler = QueueHandler(logging_queue)
logging.getLogger().addHandler(queue_handler)

queue_listener = QueueListener(logging_queue, console_handler)
queue_listener.start()


def add_handler(handler):
    global queue_listener
    new_handlers = list(queue_listener.handlers)
    new_handlers.append(handler)
    queue_listener.handlers = tuple(new_handlers)


def get_logger(logger_name: str, level: int = logging.DEBUG) -> logging.Logger:
    new_logger = logging.getLogger(name=logger_name)
    new_logger.setLevel(level=level)
    return new_logger


def stop_listener():
    global queue_listener
    queue_listener.stop()


def patch_threading_excepthook():
    """Installs our exception handler into the threading modules Thread object
    Inspired by https://bugs.python.org/issue1230540
    """
    old_init = threading.Thread.__init__

    def new_init(self, *args, **kwargs):
        old_init(self, *args, **kwargs)
        old_run = self.run

        # noinspection PyShadowingNames
        # noinspection PyBroadException
        def run_with_our_excepthook(*args, **kwargs):
            try:
                old_run(*args, **kwargs)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                sys.excepthook(*sys.exc_info())

        self.run = run_with_our_excepthook

    threading.Thread.__init__ = new_init


# Making sure unhandled exceptions logged
sys.excepthook = handle_unhandled_exception
patch_threading_excepthook()
