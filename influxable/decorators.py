import json
import requests
from . import exceptions


def raise_if_error(func):
    def func_wrapper(*args, **kwargs):
        try:
            request = args[0]
            params = kwargs.get('params', {})
            res = func(*args, **kwargs)
            try:
                json_res = res.json()
            except json.decoder.JSONDecodeError:
                json_res = {}
            res.raise_for_status()

        except requests.exceptions.MissingSchema as err:
            raise exceptions.InfluxDBInvalidURLError(request.base_url)

        except requests.exceptions.ConnectionError as err:
            raise exceptions.InfluxDBConnectionError(err)

        except requests.exceptions.HTTPError as err:
            if json_res and 'error' in json_res and\
               json_res['error'].startswith('error parsing query'):
                query = params['q']
                raise exceptions.InfluxDBBadQueryError(query)

            if json_res and 'error' in json_res and\
               json_res['error'].endswith('invalid number'):
                points = kwargs['data']
                raise exceptions.InfluxDBInvalidNumberError(points)

            if res.status_code == 400:
                raise exceptions.InfluxDBBadRequestError(params)
            if res.status_code == 401:
                raise exceptions.InfluxDBUnauthorizedError(err)
            raise err
        return res
    return func_wrapper