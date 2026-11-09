from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

ZONE = ZoneInfo("Europe/Samara")
DAY = timedelta(days=1)


def working(day, holidays):
    return day.weekday() < 5 and day not in holidays


def offer_deadline(at, holidays):
    local = at.astimezone(ZONE)
    remaining = 5
    while remaining:
        local += DAY
        if working(local.date(), holidays):
            remaining -= 1
    return local


def underwriting_deadline(at, holidays):
    cursor = at.astimezone(ZONE)
    day = cursor.date()
    remaining = timedelta(hours=1)
    while True:
        if working(day, holidays):
            for start, stop in ((9, 13), (14, 18)):
                left = max(cursor, datetime.combine(day, time(start), ZONE))
                right = datetime.combine(day, time(stop), ZONE)
                if left < right:
                    if right - left >= remaining:
                        return left + remaining
                    remaining -= right - left
        day += DAY


def holidays(db):
    return {row["day"] for row in db.execute("SELECT day FROM stand_holiday")}
