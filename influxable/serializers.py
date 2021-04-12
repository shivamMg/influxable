import json
import itertools
import pandas as pd
from .exceptions import InfluxDBInvalidResponseError
from .response import InfluxDBResponse


class BaseSerializer:
    def __init__(self, response, *args, **kwargs):
        if not isinstance(response, InfluxDBResponse):
            msg = '\'response\' must be type of InfluxDBResponse'
            raise InfluxDBInvalidResponseError(msg)
        self.response = response

    def convert(self):
        return self.response.raw


class JsonSerializer(BaseSerializer):
    def convert(self):
        return json.dumps(self.response.raw)


class FormattedSerieSerializer(BaseSerializer):
    def convert(self):
        formatted_series = []
        series = self.response.series
        for serie in series:
            name = serie.name
            columns = serie.columns
            values = serie.values
            if values is None:
                values = [[None] * len(serie.columns)]
            formatted_values = [dict(zip(columns, v)) for v in values]
            formatted_series.append({name: formatted_values})
        return formatted_series


class FlatFormattedSerieSerializer(FormattedSerieSerializer):
    def convert(self):
        formatted_series = super().convert()
        if len(formatted_series) == 1:
            main_serie = formatted_series[0]
            flat_main_serie = list(main_serie.values())[0]
            return flat_main_serie
        return []


class FlatSimpleResultSerializer(BaseSerializer):
    def convert(self):
        serie = self.response.main_serie
        values = serie.values if serie else []
        flatten_serie = list(itertools.chain(*values))
        return flatten_serie


class FlatSingleValueSerializer(FlatSimpleResultSerializer):
    def convert(self):
        simple_result = super().convert()
        if len(simple_result) == 1:
            return simple_result[0]
        return None


class PandasSerializer(BaseSerializer):
    def convert(self):
        serie = self.response.main_serie
        columns = serie.columns
        values = serie.values
        df = pd.DataFrame(values, columns=columns)
        return df


class RowColumnSerializerV0(BaseSerializer):
    def convert(self):
        serie = self.response.main_serie
        if not serie:
            return [], []
        columns = serie.columns
        tags_column = list(serie.tags.keys())
        values = []
        columns.extend(tags_column)
        for serie in self.response.series:
            for value in serie.values:
                tags = serie.tags
                for tag_colum in tags_column:
                    value.append(tags[tag_colum])
            values.extend(serie.values)
        return columns, values


class RowColumnSerializer(BaseSerializer):
    def convert(self):
        serie = self.response.main_serie
        if not serie:
            return [], []
        columns = [serie.columns[0]]
        time_values = list(map(lambda x: x[0], serie.values))  # since they are unique
        values = [time_values]
        for i, col in enumerate(serie.columns[1:], 1):
            for serie in self.response.series:
                vals = list(map(lambda x: x[i], serie.values))
                tags = getattr(serie, 'tags', {})
                if tags:
                    tags_names, tags_values = list(zip(*tags.items()))
                else:
                    tags_names, tags_values = (), ()
                columns.append((col, ('tag_names', tags_names), ('tag_values', tags_values)))
                values.append(vals)
        return columns, values


class MeasurementPointSerializer(FlatFormattedSerieSerializer):
    def __init__(self, response, measurement):
        from .measurement import MeasurementMeta
        if not isinstance(response, InfluxDBResponse):
            msg = '\'response\' must be type of InfluxDBResponse'
            raise InfluxDBInvalidResponseError(msg)
        if not isinstance(measurement, MeasurementMeta):
            msg = '\'measurement\' must be type of Measurement'
            raise InfluxDBInvalidResponseError(msg)
        self.response = response
        self.measurement = measurement

    def convert(self):
        flat_formatted_series = super().convert()
        timestamp_attributes = self.measurement._get_timestamp_attributes()
        timestamp_attributes_names = [
            ta.attribute_name
            for ta in timestamp_attributes
        ]
        self.convert_to_seconds(timestamp_attributes_names, flat_formatted_series)
        points = [self.measurement(**ffs) for ffs in flat_formatted_series]
        return points

    def convert_to_seconds(self, attr_names, series):
        NANO_TO_SEC_RATIO = 1000 * 1000 * 1000
        for field in series:
            for attr_name in attr_names:
                field[attr_name] /= NANO_TO_SEC_RATIO
