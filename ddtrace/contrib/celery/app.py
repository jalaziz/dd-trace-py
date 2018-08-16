from celery import signals

from ddtrace import Pin
from ddtrace.pin import _DD_PIN_NAME
from ddtrace.ext import AppTypes

from .util import APP, WORKER_SERVICE
from .signals import (
    trace_prerun,
    trace_postrun,
    trace_before_publish,
    trace_after_publish,
    trace_failure,
)


def patch_app(app, pin=None):
    """Attach the Pin class to the application and connect
    our handlers to Celery signals.
    """
    if getattr(app, '__datadog_patch', False):
        return
    setattr(app, '__datadog_patch', True)

    # attach the PIN object
    pin = pin or Pin(service=WORKER_SERVICE, app=APP, app_type=AppTypes.worker)
    pin.onto(app)
    # connect to the Signal framework
    signals.task_prerun.connect(trace_prerun)
    signals.task_postrun.connect(trace_postrun)
    signals.before_task_publish.connect(trace_before_publish)
    signals.after_task_publish.connect(trace_after_publish)
    signals.task_failure.connect(trace_failure)
    return app


def unpatch_app(app):
    """Remove the Pin instance from the application and disconnect
    our handlers from Celery signal framework.
    """
    if not getattr(app, '__datadog_patch', False):
        return
    setattr(app, '__datadog_patch', False)

    pin = Pin.get_from(app)
    if pin is not None:
        delattr(app, _DD_PIN_NAME)

    signals.task_prerun.disconnect(trace_prerun)
    signals.task_postrun.disconnect(trace_postrun)
    signals.before_task_publish.disconnect(trace_before_publish)
    signals.after_task_publish.disconnect(trace_after_publish)
    signals.task_failure.disconnect(trace_failure)
