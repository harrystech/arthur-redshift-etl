from datetime import datetime, timedelta
import os.path
from string import Template


class Thyme:

    def __init__(self, date: datetime):
        self.date = date

    @property
    def day(self) -> str:
        return self.date.strftime('%d')

    @property
    def month(self) -> str:
        return self.date.strftime('%m')

    @property
    def year(self) -> str:
        return self.date.strftime('%Y')

    @staticmethod
    def today() -> "Thyme":
        return Thyme(datetime.utcnow())

    @staticmethod
    def yesterday() -> "Thyme":
        y = datetime.utcnow() - timedelta(days=1)
        return Thyme(y)

    def as_path(self) -> str:
        return os.path.join(self.year, self.month, self.day)

    @staticmethod
    def render_template(prefix: str, template_string: str) -> str:
        """
        Render the template adding the prefix, yesterday and today.

        Note that you should pass in a string and we'll take care of
        the templating.

        >>> Thyme.render_template("foo", "${prefix}")
        'foo'
        """
        data = {
            "prefix": prefix,
            "yesterday": Thyme.yesterday().as_path(),
            "today": Thyme.today().as_path()
        }
        template = Template(template_string)
        return template.substitute(data)
