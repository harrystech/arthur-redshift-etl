import datetime
import decimal

import simplejson as json


class FancyJsonEncoder(json.JSONEncoder):
    """
    Fancy encoder for JSON turning instances of datetime and Decimal into something more primordial:
        Instance of datetime.datetime -> String in ISO 8601 format (with ' ', not 'T')
        Instance of decimal.Decimal -> float or int depending on presence of decimals
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat(" ")
        elif isinstance(obj, decimal.Decimal):
            if obj % 1 > 0:
                return float(obj)
            else:
                return int(obj)
        else:
            return super().default(obj)
