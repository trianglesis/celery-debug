# -*- coding: utf-8 -*-
"""Utilities related to dates, times, intervals, and timezones."""
from __future__ import absolute_import, print_function, unicode_literals

import numbers
import os
import random
import time as _time
from time import sleep
from calendar import monthrange
from datetime import date, datetime, timedelta, tzinfo

from kombu.utils.functional import reprcall
from kombu.utils.objects import cached_property
from pytz import AmbiguousTimeError, FixedOffset
from pytz import timezone as pytz_timezone
from pytz import utc

from celery.five import PY3, python_2_unicode_compatible, string_t

from .functional import dictfilter
from .iso8601 import parse_iso8601
from .text import pluralize

from django.utils import timezone

from celery.utils.log import get_logger
logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

import logging
octolog = logging.getLogger("celery.octologger")


__all__ = (
    'LocalTimezone', 'timezone', 'maybe_timedelta',
    'delta_resolution', 'remaining', 'rate', 'weekday',
    'humanize_seconds', 'maybe_iso8601', 'is_naive',
    'make_aware', 'localize', 'to_utc', 'maybe_make_aware',
    'ffwd', 'utcoffset', 'adjust_timestamp',
    'get_exponential_backoff_interval',
)

C_REMDEBUG = os.environ.get('C_REMDEBUG', False)

DAYNAMES = 'sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'
WEEKDAYS = dict(zip(DAYNAMES, range(7)))

RATE_MODIFIER_MAP = {
    's': lambda n: n,
    'm': lambda n: n / 60.0,
    'h': lambda n: n / 60.0 / 60.0,
}

TIME_UNITS = (
    ('day', 60 * 60 * 24.0, lambda n: format(n, '.2f')),
    ('hour', 60 * 60.0, lambda n: format(n, '.2f')),
    ('minute', 60.0, lambda n: format(n, '.2f')),
    ('second', 1.0, lambda n: format(n, '.2f')),
)

ZERO = timedelta(0)

_local_timezone = None


@python_2_unicode_compatible
class LocalTimezone(tzinfo):
    """Local time implementation.

    Note:
        Used only when the :setting:`enable_utc` setting is disabled.
    """

    _offset_cache = {}

    def __init__(self):
        # TODO: Check how affect! Looks like no effect!
        # This code is moved in __init__ to execute it as late as possible
        # See get_default_timezone().
        octolog.info("<=Utils Time=> __init__ _time.timezone %s", _time.timezone)
        octolog.info("<=Utils Time=> __init__ _time.daylight %s", _time.daylight)
        octolog.info("<=Utils Time=> __init__ _time.altzone %s", _time.altzone)

        # Timezone for SERVER TIME!!!!!!!!
        # But maybe we should track alt time for Celery settings timezone?

        self.STDOFFSET = timedelta(seconds=-_time.timezone)

        octolog.info("<=Utils Time=> __init__ self.STDOFFSET Standard %s", self.STDOFFSET)

        if _time.daylight:
            self.DSTOFFSET = timedelta(seconds=-_time.altzone)
            # self.DSTOFFSET = timedelta(hours=2)
            octolog.info("<=Utils Time=> __init__ self.DSTOFFSET Day light save %s", self.DSTOFFSET)
        else:
            self.DSTOFFSET = self.STDOFFSET
            # self.DSTOFFSET = timedelta(hours=0)
            octolog.info("<=Utils Time=> __init__ self.DSTOFFSET No Day light save %s", self.DSTOFFSET)

        self.DSTDIFF = self.DSTOFFSET - self.STDOFFSET

        octolog.info("<=Utils Time=> __init__ self.DSTDIFF Difference %s", self.DSTDIFF)

        tzinfo.__init__(self)

    def __repr__(self):
        # TODO: This should be +2 For London day save but real is +1!!!
        # LOCAL_TIMEZONE ret_inf <LOCAL_TIMEZONE: UTC+01>
        ret_inf = '<LOCAL_TIMEZONE: UTC{0:+03d}>'.format(int(self.DSTOFFSET.total_seconds() / 3600),)
        octolog.info("<=Utils Time=> __init__ ret_inf %s", str(ret_inf))
        return ret_inf

    def utcoffset(self, dt):
        utc_ofc = self.DSTOFFSET if self._isdst(dt) else self.STDOFFSET
        octolog.info("<=Utils Time=> LOCAL_TIMEZONE utc_ofc %s", str(utc_ofc))
        return utc_ofc

    def dst(self, dt):
        dst_ret = self.DSTDIFF if self._isdst(dt) else ZERO
        octolog.info("<=Utils Time=> LOCAL_TIMEZONE dst_ret %s", str(dst_ret))
        return dst_ret

    def tzname(self, dt):
        tz_name_v = _time.tzname[self._isdst(dt)]
        octolog.info("<=Utils Time=> LOCAL_TIMEZONE tz_name_v %s", str(tz_name_v))
        return tz_name_v

    if PY3:  # pragma: no cover

        def fromutc(self, dt):
            # The base tzinfo class no longer implements a DST
            # offset aware .fromutc() in Python 3 (Issue #2306).

            # I'd rather rely on pytz to do this, than port
            # the C code from cpython's fromutc [asksol]
            offset = int(self.utcoffset(dt).seconds / 60.0)
            octolog.info("<=Utils Time=> LOCAL_TIMEZONE offset %s", str(offset))
            try:
                tz = self._offset_cache[offset]
                octolog.info("<=Utils Time=> LOCAL_TIMEZONE tz 1 %s", str(tz))
            except KeyError:
                tz = self._offset_cache[offset] = FixedOffset(offset)
                octolog.info("<=Utils Time=> LOCAL_TIMEZONE tz 2 %s", str(tz))
            tz_fromutc = tz.fromutc(dt.replace(tzinfo=tz))
            octolog.info("<=Utils Time=> LOCAL_TIMEZONE tz_fromutc %s", str(tz_fromutc))
            return tz_fromutc

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


class _Zone(object):

    def tz_or_local(self, tzinfo=None):
        # pylint: disable=redefined-outer-name
        octolog.error("<=Time _Zone=> tz_or_local what is tzinfo: %s", str(tzinfo))
        if tzinfo is None:
            octolog.error("<=Time _Zone=> tz_or_local tzinfo is None: %s", str(self.local))
            return self.local
        return self.get_timezone(tzinfo)

    # def _to_local(self, dt, local=None, orig=None):
    #     octolog.error("<=Time _Zone=> TO_LOCAL \t what is dt: %s", dt)
    #     octolog.error("<=Time _Zone=> TO_LOCAL \t what is local: %s", local)
    #     octolog.error("<=Time _Zone=> TO_LOCAL \t what is orig: %s", orig)
    #     octolog.error("<=Time _Zone=> TO_LOCAL \t what is self.utc: %s", self.utc)
    #     if is_naive(dt):
    #         dt = make_aware(dt, orig or self.utc)
    #         octolog.error("<=Time _Zone=> TO_LOCAL \t make aware dt %s", dt)
    #         # dt = datetime.now(timezone.utc)
    #     octolog.error("<=Time _Zone=> TO_LOCAL \t localize(dt, self.tz_or_local(local)) %s", localize(dt, self.tz_or_local(local)))
    #     return localize(dt, self.tz_or_local(local))

    if PY3:  # pragma: no cover

        def to_system(self, dt):
            # tz=None is a special case since Python 3.3, and will
            # convert to the current local timezone (Issue #2306).
            octolog.error("<=Time _Zone=> to_system 1 -> PY3 dt.astimezone(tz=None) %s", str(dt.astimezone(tz=None)))
            return dt.astimezone(tz=None)

    else:

        def to_system(self, dt):  # noqa
            octolog.error("<=Time _Zone=> to_system 2 -> localize(dt, self.local) %s", str(localize(dt, self.local)))
            return localize(dt, self.local)

    def to_local_fallback(self, dt):
        # TODO: This should be +2 For London day save
        # what is dt: 2018-06-23 12:54:56.404865+01:00
        # what is self.local: <LOCAL_TIMEZONE: UTC+01>
        octolog.error("<=Time _Zone=> TO_LOCAL_FALLBACK(): what is dt: %s", str(dt))
        octolog.error("<=Time _Zone=> TO_LOCAL_FALLBACK(): what is self.local: %s", str(self.local))
        if is_naive(dt):
            octolog.error("<=Time _Zone=> to_local_fallback -> make_aware(dt, self.local): %s", str(make_aware(dt, self.local)))
            return make_aware(dt, self.local)
        return localize(dt, self.local)

    def get_timezone(self, zone):
        if isinstance(zone, string_t):
            # Something tries to get UTC instead of settings London!
            octolog.info("<=Time _Zone=>    GET_TIMEZONE(): isinstance %s", str(pytz_timezone(zone)))
            return pytz_timezone(zone)
        octolog.info("<=Time _Zone=>        GET_TIMEZONE(): %s", str(zone))
        return zone

    @cached_property
    def local(self):
        octolog.info("<=Time _Zone=> local -> LocalTimezone() %s", str(LocalTimezone()))
        return LocalTimezone()

    @cached_property
    def utc(self):
        """
        Get TZ when not app.conf.timezone   = 'Europe/London'
                        OR
                        app.conf.enable_utc   = True
        :return:
        """
        # Still get UTC even when settings use London!
        get_tz = pytz_timezone('UTC')
        # if _time.daylight:
        #     # get_tz = pytz_timezone('Europe/London')
        #     get_tz = datetime.utcnow().replace(tzinfo=utc)
        # else:
        #     get_tz = self.get_timezone('UTC')
        # Something tries to get UTC instead of settings London!
        # # Maybe def to_utc(dt_utc) to make aware and it's work fine:
        octolog.info("<=Time _Zone=>        UTC(): -> get_tz %s", str(get_tz))
        return get_tz


timezone = _Zone()
# octolog.error("<=Utils Time=> START")
# octolog.error("<=Utils Time=> timezone = _Zone() %s", str(timezone))


def maybe_timedelta(delta):
    """Convert integer to timedelta, if argument is an integer."""
    if isinstance(delta, numbers.Real):
        octolog.error("<=Utils Time=> MAYBE_TIMEDELTA isinstance(delta, numbers.Real %s", str(timedelta(seconds=delta)))
        return timedelta(seconds=delta)
    octolog.error("<=Utils Time=> MAYBE_TIMEDELTA %s", str(delta))
    return delta


def delta_resolution(dt, delta):
    """Round a :class:`~datetime.datetime` to the resolution of timedelta.

    If the :class:`~datetime.timedelta` is in days, the
    :class:`~datetime.datetime` will be rounded to the nearest days,
    if the :class:`~datetime.timedelta` is in hours the
    :class:`~datetime.datetime` will be rounded to the nearest hour,
    and so on until seconds, which will just return the original
    :class:`~datetime.datetime`.
    """
    delta = max(delta.total_seconds(), 0)

    resolutions = ((3, lambda x: x / 86400),
                   (4, lambda x: x / 3600),
                   (5, lambda x: x / 60))

    args = dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
    for res, predicate in resolutions:
        if predicate(delta) >= 1.0:
            return datetime(*args[:res], tzinfo=dt.tzinfo)
    return dt


def remaining(start, ends_in, now=None, relative=False):
    """Calculate the remaining time for a start date and a timedelta.

    For example, "how many seconds left for 30 seconds after start?"

    Arguments:
        start (~datetime.datetime): Starting date.
        ends_in (~datetime.timedelta): The end delta.
        relative (bool): If enabled the end time will be calculated
            using :func:`delta_resolution` (i.e., rounded to the
            resolution of `ends_in`).
        now (Callable): Function returning the current time and date.
            Defaults to :func:`datetime.utcnow`.

    Returns:
        ~datetime.timedelta: Remaining time.
        datetime.utcnow().replace(tzinfo=utc)
    """
    # now = now or datetime.utcnow()
    # now = now or datetime.now(timezone.utc)
    now = now or datetime.utcnow().replace(tzinfo=utc)
    # now = datetime.now()
    octolog.error("<=Utils Time=> remaining -> now %s", now)
    """
        [2018-06-22 09:04:30,588: ERROR/MainProcess] <=time.py=> remaining -> now 2018-06-22 08:04:30.588787+00:00
        [2018-06-22 09:04:30,589: ERROR/MainProcess] <=time.py=> remaining: end_date = start + ends_in 2018-06-23 04:00:00+00:00
        [2018-06-22 09:04:30,589: ERROR/MainProcess] <=time.py=> remaining: ret = end_date - now 19:55:29.411213

    """
    if now.utcoffset() != start.utcoffset():
        # octolog.error("<=time.py=> remaining: if now.utcoffset() != start.utcoffset() %s - %s", now.utcoffset(), start.utcoffset())
        # Timezone has changed, or DST started/ended
        start = start.replace(tzinfo=now.tzinfo)
        # octolog.error("<=time.py=> remaining: start = start.replace(tzinfo=now.tzinfo) %s", start)
    end_date = start + ends_in
    # octolog.error("<=time.py=> remaining: end_date = start + ends_in %s", end_date)
    if relative:
        end_date = delta_resolution(end_date, ends_in)
        # octolog.error("<=time.py=> remaining: end_date = delta_resolution(end_date, ends_in) %s", end_date)
    ret = end_date - now
    # octolog.error("<=time.py=> remaining: ret = end_date - now %s", ret)
    if C_REMDEBUG:  # pragma: no cover
        print('rem: NOW:%r START:%r ENDS_IN:%r END_DATE:%s REM:%s' % (
            now, start, ends_in, end_date, ret))
    return ret


def rate(r):
    """Convert rate string (`"100/m"`, `"2/h"` or `"0.5/s"`) to seconds."""
    if r:
        if isinstance(r, string_t):
            ops, _, modifier = r.partition('/')
            return RATE_MODIFIER_MAP[modifier or 's'](float(ops)) or 0
        return r or 0
    return 0


def weekday(name):
    """Return the position of a weekday: 0 - 7, where 0 is Sunday.

    Example:
        >>> weekday('sunday'), weekday('sun'), weekday('mon')
        (0, 0, 1)
    """
    abbreviation = name[0:3].lower()
    try:
        return WEEKDAYS[abbreviation]
    except KeyError:
        # Show original day name in exception, instead of abbr.
        raise KeyError(name)


def humanize_seconds(secs, prefix='', sep='', now='now', microseconds=False):
    """Show seconds in human form.

    For example, 60 becomes "1 minute", and 7200 becomes "2 hours".

    Arguments:
        prefix (str): can be used to add a preposition to the output
            (e.g., 'in' will give 'in 1 second', but add nothing to 'now').
        now (str): Literal 'now'.
        microseconds (bool): Include microseconds.
    """

    octolog.error("<=Utils Time=> humanize_seconds")
    secs = float(format(float(secs), '.2f'))
    for unit, divider, formatter in TIME_UNITS:
        if secs >= divider:
            w = secs / float(divider)
            pref = '{0}{1}{2} {3}'.format(prefix, sep, formatter(w),
                                          pluralize(w, unit))
            octolog.error("<=Utils Time=> humanize_seconds 2 %s", str(pref))
            return pref
    if microseconds and secs > 0.0:
        pref = '{prefix}{sep}{0:.2f} seconds'.format(
            secs, sep=sep, prefix=prefix)
        octolog.error("<=Utils Time=> humanize_seconds 2 %s", str(pref))
        return pref
    return now


def maybe_iso8601(dt):
    """Either ``datetime | str -> datetime`` or ``None -> None``."""
    if not dt:
        return
    if isinstance(dt, datetime):
        return dt
    parse = parse_iso8601(dt)
    octolog.error("<=Utils Time=> maybe_iso8601 %s", str(parse))
    return parse


def is_naive(dt):
    """Return :const:`True` if :class:`~datetime.datetime` is naive."""
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def make_aware(dt, tz):
    """Set timezone for a :class:`~datetime.datetime` object.
        An aware current UTC datetime can be obtained by calling datetime.now(timezone.utc). See also now().
    """
    # 2018-06-22 08:34:22.079659
    # make_aware What is dt:              2018-06-22 21:19:27.715681
    octolog.error("<=Utils Time=>     MAKE_AWARE(): What is dt: %s", str(dt))
    # What is tz: UTC
    # MAKE_AWARE(): What is tz:              Europe/London
    octolog.error("<=Utils Time=>     MAKE_AWARE(): What is tz: %s", str(tz))
    try:
        # <=Utils Time=>                   MAKE_AWARE(): localized = tz.localize
        #           <bound method Europe/London.localize of <DstTzInfo 'Europe/London' LMT-1 day, 23:59:00 STD>>
        localized = tz.localize
        octolog.error("<=Utils Time=>     MAKE_AWARE(): localized = tz.localize %s", str(localized))
    except AttributeError:  # 'NoneType' object has no attribute 'astimezone'
        dt_repl = dt.replace(tzinfo=tz)
        octolog.error("<=Utils Time=>     MAKE_AWARE(): dt_repl %s", str(dt_repl))
        return dt_repl
    else:  # works on pytz timezones
        try:
            # <=Utils Time=>                   MAKE_AWARE(): -> localized(dt, is_dst=None) 2018-06-22 21:29:55.121128+01:00
            loc = localized(dt, is_dst=None)
            octolog.error("<=Utils Time=>  MAKE_AWARE(): -> localized(dt, is_dst=None) %s", str(loc))
            return loc
        except AmbiguousTimeError:
            min_loc_tuple = min(localized(dt, is_dst=True), localized(dt, is_dst=False))
            octolog.error("<=Utils Time=>  MAKE_AWARE(): -> min_loc_tuple %s", str(min_loc_tuple))
            return min_loc_tuple


def localize(dt, tz):
    """Convert aware :class:`~datetime.datetime` to another timezone."""
    octolog.error("<=Utils Time=>     LOCALIZE(): What is dt: %s", str(dt))
    octolog.error("<=Utils Time=>     LOCALIZE(): What is tz: %s", str(tz))

    if is_naive(dt):  # Ensure timezone aware datetime
        dt = make_aware(dt, tz)
        octolog.error("<=Utils Time=>     LOCALIZE(): is_naive dt: %s", str(dt))

    if dt.tzinfo == utc:
        dt = dt.astimezone(tz)  # Always safe to call astimezone on utc zones
        octolog.error("<=Utils Time=>     LOCALIZE(): dt.tzinfo == utc dt: %s", str(dt))

    try:
        _normalize = tz.normalize
        octolog.error("<=Utils Time=>     LOCALIZE(): _normalize tz: %s", str(tz))

    except AttributeError:  # non-pytz tz
        octolog.error("<=Utils Time=>     LOCALIZE(): AttributeError: non-pytz tz dt: %s", str(dt))
        return dt

    else:
        try:
            norm_dt = _normalize(dt, is_dst=None)
            octolog.error("<=Utils Time=>     LOCALIZE(): esle _normalize(dt, is_dst=None): %s", norm_dt)
            return norm_dt

        except TypeError:
            norm_dt = _normalize(dt)
            octolog.error("<=Utils Time=>     LOCALIZE(): TypeError: _normalize(dt): %s", norm_dt)
            return norm_dt

        except AmbiguousTimeError:
            min_dt = min(_normalize(dt, is_dst=True), _normalize(dt, is_dst=False))
            octolog.error("<=Utils Time=>     LOCALIZE(): AmbiguousTimeError: %s", str(min_dt))
            return min_dt


def to_utc(dt_utc):
    """Convert naive :class:`~datetime.datetime` to UTC.
        TO_UTC(): -> What is dt_utc: 2018-06-23 18:47:39.529758
        TO_UTC(): -> timezone.utc: UTC
        TO_UTC(): -> make_aware(dt_utc, timezone.utc): 2018-06-23 18:47:39.529758+00:00

    """
    octolog.error("<=Utils Time=>     TO_UTC(): -> What is dt_utc: %s", str(dt_utc))
    octolog.error("<=Utils Time=>     TO_UTC(): -> timezone.utc: %s", str(timezone.utc))
    octolog.error("<=Utils Time=>     TO_UTC(): -> make_aware(dt_utc, timezone.utc): %s", str(make_aware(dt_utc, timezone.utc)))
    return make_aware(dt_utc, timezone.utc)


def maybe_make_aware(dt, tz=None):
    """Convert dt to aware datetime, do nothing if dt is already aware."""
    octolog.error("<=Utils Time=>     MAYBE_MAKE_AWARE(): -> What is dt: %s", str(dt))
    if is_naive(dt):
        dt = to_utc(dt)
        octolog.error("<=Utils Time=>     MAYBE_MAKE_AWARE(): dt = to_utc(dt) -> dt: %s", str(dt))

        loc_v = localize(dt, timezone.utc if tz is None else timezone.tz_or_local(tz),)
        octolog.error("<=Utils Time=>     MAYBE_MAKE_AWARE(): loc_v -> loc_v: %s", loc_v)
        return loc_v
    return dt


@python_2_unicode_compatible
class ffwd(object):
    """Version of ``dateutil.relativedelta`` that only supports addition."""

    def __init__(self, year=None, month=None, weeks=0, weekday=None, day=None,
                 hour=None, minute=None, second=None, microsecond=None,
                 **kwargs):
        # pylint: disable=redefined-outer-name
        # weekday is also a function in outer scope.
        self.year = year
        self.month = month
        self.weeks = weeks
        self.weekday = weekday
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.microsecond = microsecond
        self.days = weeks * 7
        self._has_time = self.hour is not None or self.minute is not None

        # Looks like cron
        # octolog.error("<=Utils Time=>             ffwd self._has_time: %s", self._has_time)
        # octolog.error("<=Utils Time=>             ffwd self.year: %s", self.year)
        # octolog.error("<=Utils Time=>             ffwd self.month: %s", self.month)
        # octolog.error("<=Utils Time=> ffwd self.weeks: %s", self.weeks)
        # octolog.error("<=Utils Time=> ffwd self.weekday: %s", self.weekday)
        # octolog.error("<=Utils Time=> ffwd self.day: %s", self.day)
        # octolog.error("<=Utils Time=> ffwd self.hour: %s", self.hour)
        # octolog.error("<=Utils Time=> ffwd self.minute: %s", self.minute)

    def __repr__(self):
        return reprcall('ffwd', (), self._fields(weeks=self.weeks,
                                                 weekday=self.weekday))

    def __radd__(self, other):
        if not isinstance(other, date):
            return NotImplemented
        year = self.year or other.year
        month = self.month or other.month
        day = min(monthrange(year, month)[1], self.day or other.day)
        ret = other.replace(**dict(dictfilter(self._fields()),
                                   year=year, month=month, day=day))
        if self.weekday is not None:
            ret += timedelta(days=(7 - ret.weekday() + self.weekday) % 7)
        return ret + timedelta(days=self.days)

    def _fields(self, **extra):
        return dictfilter({
            'year': self.year, 'month': self.month, 'day': self.day,
            'hour': self.hour, 'minute': self.minute,
            'second': self.second, 'microsecond': self.microsecond,
        }, **extra)


def utcoffset(time=_time, localtime=_time.localtime):
    """
        Return the current offset to UTC in hours.
        https://github.com/celery/celery/issues/4842
    """
    # tm_isdst_v: 1
    if localtime().tm_isdst:
        # No effect:
        time_altzone = time.altzone // 3600
        # time_altzone day time.altzone: -3600
        # octolog.info("time_altzone day time.altzone: %s", time.altzone)
        # time_altzone day savetime delta: -1
        # octolog.info("time_altzone day savetime delta: %s", time_altzone)

        time_timezone = time.timezone // 3600 * 2
        # time_altzone day time.timezone: 0
        # octolog.info("time_altzone day time.timezone: %s", time.timezone)
        # time_timezone not day savetime delta: 0
        # octolog.info("time_timezone not day savetime delta: %s", time_timezone)

        return time_altzone

    # Slould be: time_timezone not day savetime delta: -2
    time_timezone = time.timezone // 3600
    return time_timezone


def adjust_timestamp(ts, offset, here=utcoffset):
    """Adjust timestamp based on provided utcoffset."""

    adjust_ts = ts - (offset - here()) * 3600
    # octolog.info("adjust_ts ts: %s", ts)
    # octolog.info("adjust_ts offset: %s", offset)
    # octolog.info("adjust_ts here: %s", here)
    # octolog.info("adjust_ts: adjust_ts %s", adjust_ts)
    # sleep(2)
    # return -2
    return adjust_ts


def get_exponential_backoff_interval(
    factor,
    retries,
    maximum,
    full_jitter=False
):
    """Calculate the exponential backoff wait time."""
    # Will be zero if factor equals 0
    countdown = factor * (2 ** retries)
    # Full jitter according to
    # https://www.awsarchitectureblog.com/2015/03/backoff.html
    if full_jitter:
        countdown = random.randrange(countdown + 1)
    # Adjust according to maximum wait time and account for negative values.
    return max(0, min(maximum, countdown))
