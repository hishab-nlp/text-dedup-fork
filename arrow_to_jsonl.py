import os
import json
from tqdm import tqdm
from datasets import load_dataset

# Specify the path to your Arrow file
data_fir = "/data2/deduplication/dedup_output/query_url_tld_bd_cc_2017_2023/arrow_files"
cache_dir = "/data2/deduplication/cached"
output_base_path = "/data2/deduplication/dedup_output/query_url_tld_bd_cc_2017_2023/jsonl_files"
os.makedirs(output_base_path, exist_ok=True)

num_shards = 20
# Load the dataset from the Arrow file
dataset = load_dataset('arrow', data_dir=data_fir, cache_dir=cache_dir)

# Splitting the dataset into 10 shards
shards = [dataset['train'].shard(num_shards=num_shards, index=i) for i in range(num_shards)]

for i, shard in tqdm(enumerate(shards), total=len(shards)):
    output_file = os.path.join(output_base_path, f"chunk_{i}.jsonl")
    with open(output_file, 'w') as f:
        for data in shard:
            json.dump(data, f, ensure_ascii=False)
            f.write('\n')