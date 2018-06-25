"""Utilities."""
# -- XXX This module must not use translation as that causes
# -- a recursive loader import!
from __future__ import absolute_import, unicode_literals

from django.conf import settings
from django.utils import timezone

import logging
octolog = logging.getLogger("django_celery_results.octologger")
# see Issue celery/django-celery#222
now_localtime = getattr(timezone, 'template_localtime', timezone.localtime)
octolog.error("<=DJ Celery=> now_localtime: %s", now_localtime)
octolog.error("<=DJ Celery=> timezone: %s", timezone)
octolog.error("<=DJ Celery=> timezone.localtime: %s", timezone.localtime)


def now():
    """Return the current date and time."""
    if getattr(settings, 'USE_TZ', False):
        octolog.error("<=DJ Celery=> settings: %s", settings)
        octolog.error("<=DJ Celery=> timezone.now(): %s", timezone.now())
        octolog.error("<=DJ Celery=> now_localtime(timezone.now()): %s", now_localtime(timezone.now()))
        return now_localtime(timezone.now())
    else:
        octolog.error("<=DJ Celery=> timezone.now(): %s", timezone.now())
        return timezone.now()
