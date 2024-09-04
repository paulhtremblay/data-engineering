https://github.com/aFrag/PubsubToBigQuery/blob/main/StreamingPubsubToBQ.py
 with beam.Pipeline(options=pipeline_options) as pipeline:
        realtime_data = (
                pipeline
                | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=options.input_subscription, with_attributes=True)  # Note the with_attributes here , explanation on the next step
                | "Clean Messages" >> beam.ParDo(CleanDataForBQ())
                | f"Write to {options.bq_table}" >> beam.io.WriteToBigQuery(
                    table=f"{options.bq_dataset}.{options.bq_table}",
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_NEVER
                )
        )

        (
            realtime_data[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
            | f"Window" >> GroupWindowsIntoBatches(window_size=options.bq_window_size)
            | f"Failed Rows for {options.bq_table}" >> beam.ParDo(ModifyBadRows(options.bq_dataset, options.bq_table))
        )

