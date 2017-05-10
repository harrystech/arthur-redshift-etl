from datetime import datetime, timedelta
import os.path
import string


class Thyme:

    def __init__(self, date: datetime) -> None:
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
        return Thyme(datetime.utcnow() - timedelta(days=1))

    def as_path(self) -> str:
        return os.path.join(self.year, self.month, self.day)

    @staticmethod
    def render_template(template_string: str, values: dict) -> str:
        """
        Render the template using the given values and also "yesterday" and "today".

        Note that you should pass in a string as the "template" and we'll take care of
        the actual creation of a template.

        >>> Thyme.render_template("Hello ${name}", {"name": "world!"})
        'Hello world!'
        >>> today = Thyme.render_template("${prefix}/${today}", {"prefix": "breakfast"})
        >>> today.startswith('breakfast')
        True
        """
        data = dict(values, yesterday=Thyme.yesterday().as_path(), today=Thyme.today().as_path())
        template = string.Template(template_string)
        return template.substitute(data)
