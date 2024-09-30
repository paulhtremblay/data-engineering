from apache_beam import Pipeline
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081",
    "--environment_type=LOOPBACK"
])

def run():
    with Pipeline(options = options) as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | 'print' >> beam.ParDo(print)
                )
if __name__ == '__main__':
    run()
