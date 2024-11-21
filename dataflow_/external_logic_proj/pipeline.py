from apache_beam import Pipeline
import apache_beam as beam
from dependencies.is_outlier import is_outlier
DATA= (   (   'dept1',
        [   2.3,
            4.3,
            5.6,
            7.6,
            7.4,
            11.9,
            10.9,
            15.6,
            19.7,
            21.2,
            23.4,
            26.3,
            25.7,
            30.1,
            29.9,
            33.4,
            34.8,
            36.6,
            38.6,
            41.1,
            41.0,
            45.7,
            47.5,
            48.9,
            48.1,
            54.3,
            57.8,
            59.2,
            60.1,
            61.4]),
    (   'dept2',
        [   2.2,
            3.2,
            5.3,
            6.9,
            10.8,
            13.2,
            16.1,
            16.0,
            18.0,
            23.8,
            21.8,
            25.4,
            27.3,
            28.2,
            32.6,
            34.0,
            36.7,
            40.4,
            42.3,
            43.3,
            45.6,
            47.5,
            48.5,
            52.8,
            54.0,
            58.9,
            58.6,
            60.1,
            61.7,
            64.8]),
    (   'dept3',
        [   1.4,
            4.7,
            7.9,
            9.2,
            11.7,
            14.2,
            15.4,
            16.7,
            20.2,
            20.2,
            25.6,
            27.9,
            28.6,
            32.1,
            36.2,
            35.7,
            37.8,
            42.0,
            44.6,
            45.6,
            48.7,
            51.1,
            53.0,
            54.3,
            58.0,
            59.1,
            62.7,
            63.2,
            65.3,
            73.0]))

class LinearRegression(beam.DoFn):
    """
    example
    """

    def process(self, element):
        is_o = is_outlier(element[1])
        yield element


def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
        | beam.ParDo(LinearRegression())
            | 'print' >> beam.ParDo(print)
                )
if __name__ == '__main__':
    run()
