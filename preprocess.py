import os
import glob
import json
from tqdm import tqdm
import concurrent
from concurrent.futures import ThreadPoolExecutor

cpu_count = os.cpu_count()

all_files = glob.glob('/data2/filtered_data/query_cl_ben_crawl_2017_04_to_2019_30_and_2023_50/bn_passed_normaliezd/*.jsonl')
print(f"total files: {len(all_files)}")

output_path = "/data2/deduplication/dedup_input/query_cl_ben_crawl_2017_04_to_2019_30_and_2023_50"
os.makedirs(output_path, exist_ok=True)

query_chunk_name = "query_cl_ben_crawl_2017_04_to_2019_30_and_2023_50"

def create_sublists(input_list, size):
    return [input_list[i:i + size] for i in range(0, len(input_list), size)]

def process(file):
    output = []
    with open(file) as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            data = json.loads(line)
            meta = f"query_name_{query_chunk_name}_chunk_name_{os.path.basename(file)}_line_idx_{i}"
            new_data = {'text': data['text'], 'meta': meta}
            output.append(new_data)

    return file, output

sub_files = create_sublists(all_files, 100)

for files in tqdm(sub_files):
    with ThreadPoolExecutor(max_workers=cpu_count) as executor:
        futures = [executor.submit(process, file) for file in files]

        # Using tqdm to display progress
        for future in concurrent.futures.as_completed(futures):
            file, output = future.result()
            output_filename = os.path.join(output_path, os.path.basename(file))
            with open(output_filename, 'w') as fw:
                for data in output:
                    json.dump(data, fw, ensure_ascii=False)
                    fw.write('\n')
