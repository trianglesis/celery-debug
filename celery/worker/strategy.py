# -*- coding: utf-8 -*-
"""Task execution strategy (optimization)."""
from __future__ import absolute_import, unicode_literals

import logging

from kombu.asynchronous.timer import to_timestamp
from kombu.five import buffer_t

from celery.exceptions import InvalidTaskError
from celery.utils.imports import symbol_by_name
from celery.utils.log import get_logger
from celery.utils.saferepr import saferepr
from celery.utils.time import timezone  #strategy

from .request import create_request_cls
from .state import task_reserved

__all__ = ('default',)

logger = get_logger(__name__)

import logging
octolog = logging.getLogger("celery.octologger")

# # worker.strategy celery.utils.time <celery.utils.time._Zone object at 0x7f38dfaa9dd8>
# octolog.error("<=Worker Strategy=> worker.strategy celery.utils.time timezone %s", timezone)
# # get_timezone', 'local', 'to_local_fallback', 'to_system', 'tz_or_local', 'utc'
# try:
#     octolog.error("<=Worker Strategy=> timezone.get_timezone %s", timezone.get_timezone)
#     # octolog.error("<=Worker Strategy=> timezone.local %s", timezone.local)
#     octolog.error("<=Worker Strategy=> timezone.to_local_fallback %s", timezone.to_local_fallback)
#     octolog.error("<=Worker Strategy=> timezone.to_system %s", timezone.to_system)
#     octolog.error("<=Worker Strategy=> timezone.tz_or_local %s", timezone.tz_or_local)
#     octolog.error("<=Worker Strategy=> timezone.utc %s", timezone.utc)
# except Exception as e:
#     octolog.error(e)

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.


def hybrid_to_proto2(message, body):
    """Create a fresh protocol 2 message from a hybrid protocol 1/2 message."""
    try:
        args, kwargs = body.get('args', ()), body.get('kwargs', {})
        kwargs.items  # pylint: disable=pointless-statement
    except KeyError:
        raise InvalidTaskError('Message does not have args/kwargs')
    except AttributeError:
        raise InvalidTaskError(
            'Task keyword arguments must be a mapping',
        )

    headers = {
        'lang': body.get('lang'),
        'task': body.get('task'),
        'id': body.get('id'),
        'root_id': body.get('root_id'),
        'parent_id': body.get('parent_id'),
        'group': body.get('group'),
        'meth': body.get('meth'),
        'shadow': body.get('shadow'),
        'eta': body.get('eta'),
        'expires': body.get('expires'),
        'retries': body.get('retries'),
        'timelimit': body.get('timelimit'),
        'argsrepr': body.get('argsrepr'),
        'kwargsrepr': body.get('kwargsrepr'),
        'origin': body.get('origin'),
    }

    embed = {
        'callbacks': body.get('callbacks'),
        'errbacks': body.get('errbacks'),
        'chord': body.get('chord'),
        'chain': None,
    }

    octolog.error("<=Worker Strategy=> HYBRID_TO_PROTO2(): body.get('utc', 'GOT_True') %s", body.get('utc', 'GOT_True'))

    return (args, kwargs, embed), headers, True, body.get('utc', True)


def proto1_to_proto2(message, body):
    """Convert Task message protocol 1 arguments to protocol 2.

    Returns:
        Tuple: of ``(body, headers, already_decoded_status, utc)``
    """
    try:
        args, kwargs = body.get('args', ()), body.get('kwargs', {})
        kwargs.items  # pylint: disable=pointless-statement
    except KeyError:
        raise InvalidTaskError('Message does not have args/kwargs')
    except AttributeError:
        raise InvalidTaskError(
            'Task keyword arguments must be a mapping',
        )
    body.update(
        argsrepr=saferepr(args),
        kwargsrepr=saferepr(kwargs),
        headers=message.headers,
    )
    try:
        body['group'] = body['taskset']
    except KeyError:
        pass
    embed = {
        'callbacks': body.get('callbacks'),
        'errbacks': body.get('errbacks'),
        'chord': body.get('chord'),
        'chain': None,
    }
    octolog.error("<=Worker Strategy=> PROTO1_TO_PROTO2(): body.get('utc', 'GOT_True') %s", body.get('utc', 'GOT_True'))
    return (args, kwargs, embed), body, True, body.get('utc', True)


def default(task, app, consumer,
            info=logger.info, error=logger.error, task_reserved=task_reserved,
            to_system_tz=timezone.to_system, bytes=bytes, buffer_t=buffer_t,
            proto1_to_proto2=proto1_to_proto2):
    """Default task execution strategy.

    Note:
        Strategies are here as an optimization, so sadly
        it's not very easy to override.
    """
    hostname = consumer.hostname
    connection_errors = consumer.connection_errors
    _does_info = logger.isEnabledFor(logging.INFO)

    octolog.error("<=Worker Strategy=> DEFAULT(): timezone.to_system %s", timezone.to_system)

    # task event related
    # (optimized to avoid calling request.send_event)
    eventer = consumer.event_dispatcher
    events = eventer and eventer.enabled
    send_event = eventer.send
    task_sends_events = events and task.send_events

    call_at = consumer.timer.call_at
    apply_eta_task = consumer.apply_eta_task
    rate_limits_enabled = not consumer.disable_rate_limits
    get_bucket = consumer.task_buckets.__getitem__
    handle = consumer.on_task_request
    limit_task = consumer._limit_task
    limit_post_eta = consumer._limit_post_eta
    body_can_be_buffer = consumer.pool.body_can_be_buffer
    Request = symbol_by_name(task.Request)
    Req = create_request_cls(Request, task, consumer.pool, hostname, eventer)

    octolog.info("<=Worker Strategy=> DEFAULT(): call_at: %s", call_at)
    octolog.info("<=Worker Strategy=> DEFAULT(): apply_eta_task: %s", apply_eta_task)
    octolog.info("<=Worker Strategy=> DEFAULT(): rate_limits_enabled: %s", rate_limits_enabled)
    octolog.info("<=Worker Strategy=> DEFAULT(): get_bucket: %s", get_bucket)
    octolog.info("<=Worker Strategy=> DEFAULT(): handle: %s", handle)
    octolog.info("<=Worker Strategy=> DEFAULT(): limit_task: %s", limit_task)
    octolog.info("<=Worker Strategy=> DEFAULT(): limit_post_eta: %s", limit_post_eta)
    octolog.info("<=Worker Strategy=> DEFAULT(): body_can_be_buffer: %s", body_can_be_buffer)
    octolog.info("<=Worker Strategy=> DEFAULT(): Request: %s", Request)
    octolog.info("<=Worker Strategy=> DEFAULT(): Req: %s", Req)

    revoked_tasks = consumer.controller.state.revoked

    def task_message_handler(message, body, ack, reject, callbacks,
                             to_timestamp=to_timestamp):
        if body is None and 'args' not in message.payload:
            body, headers, decoded, utc = (message.body, message.headers, False, app.uses_utc_timezone(),)

            octolog.error('<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default 1 utc=%s', utc)  # utc=False
            if not body_can_be_buffer:
                body = bytes(body) if isinstance(body, buffer_t) else body
        else:
            if 'args' in message.payload:
                body, headers, decoded, utc = hybrid_to_proto2(message, message.payload)
                octolog.error('<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default 2 utc=%s', utc)
            else:
                body, headers, decoded, utc = proto1_to_proto2(message, body)
                octolog.error('<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default 3 utc=%s', utc)

        req = Req(
            message,
            on_ack=ack, on_reject=reject, app=app, hostname=hostname,
            eventer=eventer, task=task, connection_errors=connection_errors,
            body=body, headers=headers, decoded=decoded, utc=utc,
        )
        if _does_info:
            info('Received task: %s utc=%s', req, utc)
            msg = 'Received task: message, ' \
                  '\n\ton_ack={on_ack}, \n\ton_reject={on_reject}, \n\tapp={app}, \n\thostname={hostname}, \n\t' \
                  'eventer={eventer}, \n\ttask={task}, \n\tconnection_errors={connection_errors}, \n\t' \
                  'body={body}, \n\theaders={headers}, \n\tdecoded={decoded}, \n\tutc={utc}'.format(
                message, on_ack=ack, on_reject=reject, app=app, hostname=hostname,
                eventer=eventer, task=task, connection_errors=connection_errors,
                body=body, headers=headers, decoded=decoded, utc=utc)
            octolog.error(msg)
        if (req.expires or req.id in revoked_tasks) and req.revoked():
            return

        if task_sends_events:
            send_event(
                'task-received',
                uuid=req.id, name=req.name,
                args=req.argsrepr, kwargs=req.kwargsrepr,
                root_id=req.root_id, parent_id=req.parent_id,
                retries=req.request_dict.get('retries', 0),
                eta=req.eta and req.eta.isoformat(),
                expires=req.expires and req.expires.isoformat(),
            )

        bucket = None
        eta = None
        if req.eta:
            try:
                if req.utc:
                    eta = to_timestamp(to_system_tz(req.eta))
                    octolog.error("<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default eta %s", eta)
                else:
                    eta = to_timestamp(req.eta, app.timezone)
                    octolog.error("<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default eta %s", eta)
            except (OverflowError, ValueError) as exc:
                error("Couldn't convert ETA %r to timestamp: %r. Task: %r",
                      req.eta, exc, req.info(safe=True), exc_info=True)
                req.reject(requeue=False)
        if rate_limits_enabled:
            bucket = get_bucket(task.name)
            octolog.error("<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default bucket %s", bucket)

        if eta and bucket:
            consumer.qos.increment_eventually()
            octolog.error("<=Worker Strategy=> TASK_MESSAGE_HANDLER(): default eta and bucket %s %s", eta, bucket)
            return call_at(eta, limit_post_eta, (req, bucket, 1),
                           priority=6)
        if eta:
            consumer.qos.increment_eventually()
            call_at(eta, apply_eta_task, (req,), priority=6)
            return task_message_handler
        if bucket:
            return limit_task(req, bucket, 1)

        task_reserved(req)
        if callbacks:
            [callback(req) for callback in callbacks]
        handle(req)
    return task_message_handler
