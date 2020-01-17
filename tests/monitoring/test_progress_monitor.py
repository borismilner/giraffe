import json
import random
from datetime import datetime

from giraffe.monitoring.ingestion_request import IngestionRequest
from giraffe.monitoring.progress_monitor import ProgressMonitor
from giraffe.monitoring.progress_monitor import RequestStatus

request_id = 'my-request-simple'
request_type = 'NODES'
request_content = '{"blah" : "blah"}'


def test_progress_monitor_simple_scenario(config_helper):
    pm = ProgressMonitor(config=config_helper)
    assert isinstance(pm.all_tasks, dict)
    assert not pm.all_tasks  # Dictionary is emtpy

    # Task started

    pm.task_started(request_id=request_id,
                    request_type=request_type,
                    request_content=request_content)

    assert pm.all_tasks
    assert len(pm.all_tasks.keys()) == 1
    assert request_id in pm.all_tasks

    task = pm.get_task(task_id=request_id)
    assert isinstance(task, IngestionRequest)
    assert task.status == RequestStatus.STARTED

    now_timestamp = int(datetime.now().timestamp())
    assert task.start_time_unix is not None
    assert now_timestamp - 1 <= task.start_time_unix <= now_timestamp

    task.set_status(status=RequestStatus.DONE)
    assert task.status == RequestStatus.DONE
    now_timestamp = int(datetime.now().timestamp())
    assert task.end_time_unix is not None
    assert now_timestamp - 1 <= task.end_time_unix <= now_timestamp

    for status in RequestStatus:
        task.set_status(status)
        assert task.status == status


def test_progress_monitor_error_registering(config_helper):
    pm = ProgressMonitor(config=config_helper)
    pm.task_started(request_id=request_id, request_type=request_type, request_content=request_content)

    task = pm.get_task(task_id=request_id)
    task.set_status(RequestStatus.READY_TO_WRITE_FROM_REDIS_INTO_NEO)

    try:
        throw_error()
    except Exception as exception:
        pm.error(request_id=request_id, message='Oh noez !!!', exception=exception)

    task = pm.get_task(task_id=request_id)

    assert task.status == RequestStatus.ERROR
    assert len(task.errors) == 1
    error_raising_method_name = throw_error.__name__
    assert f'in {error_raising_method_name}' in task.errors[0]

    for _ in range(0, 9):
        try:
            task.set_status(RequestStatus.FINISHED_READING_DATA_AND_MODELS)
            throw_error()
        except Exception as exception:
            pm.error(request_id=request_id, message='Oh noez !!!', exception=exception)

    assert task.status == RequestStatus.ERROR
    assert len(task.errors) == 10
    assert all(f'in {error_raising_method_name}' in error for error in task.errors)


def test_progress_monitor_operations(config_helper):
    pm = ProgressMonitor(config=config_helper)
    pm.task_started(request_id=request_id, request_type=request_type, request_content=request_content)
    now_timestamp = int(datetime.now().timestamp())
    task = pm.get_task(task_id=request_id)
    json_task = task.as_json()
    from_json = json.loads(json_task)
    assert from_json['status'] == str(RequestStatus.STARTED)
    assert from_json['id'] == request_id
    assert from_json['request_body'] == request_content
    assert isinstance(from_json['start_time_unix'], int)
    assert now_timestamp - 1 <= int(from_json['start_time_unix']) <= now_timestamp


# Not a unit-tests
def throw_error():
    i = random.randint(1, 3)
    message = 'Baaaaah!!!!'
    if i == 1:
        raise RuntimeError(message)
    elif i == 2:
        raise TimeoutError(message)
    elif i == 3:
        raise FileNotFoundError(message)
    elif i == 4:
        raise EOFError(message)
