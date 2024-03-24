python -m text_dedup.minhash \
  --path "json" \
  --data_dir "/data2/deduplication/dedup_input/query_url_tld_bd_cc_2017_2023/preprocessed" \
  --split "train" \
  --cache_dir "/data2/deduplication/cached" \
  --output "/data2/deduplication/dedup_output/query_url_tld_bd_cc_2017_2023" \
  --column "text" \
  --batch_size 10000