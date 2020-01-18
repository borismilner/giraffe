from giraffe.helpers.EventDispatcher import EventDispatcher
from giraffe.helpers.EventDispatcher import GiraffeEvent


def test_event_dispatcher(config_helper):
    callback_was_called = False
    event: GiraffeEvent = None

    def do_something(callback_event: GiraffeEvent):
        nonlocal callback_was_called, event
        event = callback_event
        callback_was_called = True

    ed = EventDispatcher()
    ed.register_callback(callback=do_something)

    request_id = 'x'
    event_message = request_id
    event_type = 0
    event_arguments = {request_id: 1}

    ed.dispatch_event(event=GiraffeEvent(request_id=request_id, event_type=event_type, message=event_message, arguments=event_arguments))
    assert callback_was_called
    assert event is not None
    assert event.request_id == request_id
    assert event.event_type == event_type
    assert event.message == event_message
    assert event.arguments == event_arguments
