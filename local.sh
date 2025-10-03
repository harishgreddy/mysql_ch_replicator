 python /home/harish/Projects/livine/livine-be/mysql_ch_replicator/main.py run_all --config config.yaml

python -m mysql_ch_replicator bulk_insert \
  --config config.yaml \
  --db lifeline_prod \
  --target_db lifeline_insights_prod \
  --table aggregated_stock_summary_wrapper
  --threads 1
  --batch_size 100000