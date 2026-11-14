from datetime import datetime, timedelta, timezone

class Clock:
    def now(self):
        return datetime.now(timezone.utc)

class FakeClock(Clock):
    def __init__(self, start=None):
        self.current = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        if isinstance(self.current, str):
            self.current = datetime.fromisoformat(self.current.replace('Z', '+00:00'))
        if self.current.tzinfo is None:
            raise ValueError('clock must be timezone aware')

    def now(self):
        return self.current

    def advance(self, seconds=0, **kwargs):
        self.current += timedelta(seconds=seconds, **kwargs)
        return self.current

def timestamp(value):
    if value.tzinfo is None:
        raise ValueError('clock must be timezone aware')
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds')
