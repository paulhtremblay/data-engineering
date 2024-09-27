from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms import window
import apache_beam as beam
import datetime

from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.options.pipeline_options import PipelineOptions
import time
def run():
    def pair_account_ids(
            api_key , account_ids   ) :
        if api_key not in account_ids:
            return None

        return (api_key, account_ids[api_key], int(time.time()))

    def echo(elm) :
        print(elm)
        return elm

    def api_keys(elm) :
        return {"<api_key_1>": "<account_id_1>", "<api_key_2>": "<account_id_2>"}

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(
        options=pipeline_options, runner=beam.runners.DirectRunner()
    ) as p:
        side_input = (
            p
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=time.time(),
                stop_timestamp=time.time() + 1,
                fire_interval=10,
                apply_windowing=True,
            )
            | "api_keys" >> beam.Map(api_keys)
        )

        main_input = (
            p
            | "MpImpulse"
            >> beam.Create(["<api_key_1>", "<api_key_2>", "<unknown_api_key>"])
            | "MapMpToTimestamped"
            >> beam.Map(lambda src: TimestampedValue(src, time.time()))
            | "WindowMpInto" >> beam.WindowInto(beam.window.FixedWindows(10))
        )

        result = (
            main_input
            | "Pair with AccountIDs"
            >> beam.Map(
                pair_account_ids, account_ids=beam.pvalue.AsSingleton(side_input)
            )
            | "filter" >> beam.Filter(lambda x: x is not None)
            | "echo 2"
            >> beam.Map(lambda x: print(f"{int(time.time())}: {x}"))
        )
    print(f"done:  {int(time.time())}")


if __name__ == "__main__":
    run()
