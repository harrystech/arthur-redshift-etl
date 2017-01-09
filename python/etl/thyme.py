from datetime import datetime, timedelta


class Thyme:

    def __init__(self, date: datetime):
        self.date = date

    @property
    def day(self) -> str:
        return self.date.strftime('%d')

    @property
    def month(self):
        return self.date.strftime('%m')

    @property
    def year(self):
        return self.date.strftime('%Y')

    @classmethod
    def today(cls):
        return Thyme(datetime.utcnow())

    @classmethod
    def yesterday(cls):
        y = datetime.utcnow() - timedelta(days=1)
        return Thyme(y)
