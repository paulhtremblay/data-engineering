
	python readbq_to_writebq_monthly.py --project greenfield_prod \
		--runner DataflowRunner \
		--temp_location  gs://bbyus-integrate-rds-myads-p01-dataflow/temp/common_attribution_split_revenue_ptc \
		--runner DataflowRunner \
		--template_location gs://bbyus-integrate-rds-myads-p01-dataflow/templates/common_attribution_split_revenue_ptc \
		--dataset_id "attribution" \
                --table_name "common_attribution_split_revenue_ptc"\
        	--setup_file ../pipeline/setup.py
