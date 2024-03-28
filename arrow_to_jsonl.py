import os
import json
from tqdm import tqdm
from datasets import load_dataset
import multiprocessing

data_dir = "/data2/output/arrows"
cache_dir = "/data2/cached"
output_base_path = "/data2/output/cc_culturax"
os.makedirs(output_base_path, exist_ok=True)

num_shards = 5000
dataset = load_dataset('arrow', data_dir=data_dir, cache_dir=cache_dir)

def process_shard(index):
    shard = dataset['train'].shard(num_shards=num_shards, index=index)
    output_file = os.path.join(output_base_path, f"chunk_{index}.jsonl")
    with open(output_file, 'w') as f:
        for data in tqdm(shard, total=len(shard)):
            meta = data['meta']
            json.dump(data, f, ensure_ascii=False)
            f.write('\n')

if __name__ == "__main__":
    # Using Pool to manage multiple processes
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        list(tqdm(pool.imap(process_shard, range(num_shards)), total=num_shards))
