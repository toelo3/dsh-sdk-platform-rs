#!/usr/bin/env python3

import random
import string
import json
from kafka.partitioner import murmur2

def generate_random_topic(min_depth=1, max_depth=10):
    depth = random.randint(min_depth, max_depth)
    parts = []
    for _ in range(depth):
        length = random.randint(1,15)
        part = ''.join(random.choices(string.ascii_lowercase + string.digits + '_', k = length))
        parts.append(part)
    return "/".join(parts)

# Source: https://docs.kpn-dsh.com/getting-started/python/dsh-stream-produce/#python-script
# Calculate the partition for a given message, using the MQTT topic, and the DSH stream's topic level depth and number of partitions.
# (1) Reduce the MQTT topic to the number of levels defined in the DSH stream's topic level depth.
# (2) Hash the reduced MQTT topic, and add a bitmask. Then apply the modulo operation to it, with the DSH stream's number of partitions as divisor.
def dsh_partitioner(key, topic_depth, partition_count):
    key_depth = '/'.join( key.split('/')[:(topic_depth)])
    return (murmur2(key_depth.encode('utf8')) & 0x7fffffff) % partition_count

def generate_golden_data(output_file="golden_data.json", num_cases=1000):
    data = []
    # Random cases
    for _ in range(num_cases):
        topic = generate_random_topic(max_depth=15)
        depth = random.randint(1, 15)
        partitions = random.choice([1, 3, 10, 100, 1024, 2048]) # Common partition counts
        
        try:
            res = dsh_partitioner(topic, depth, partitions)
            data.append({
                "input": {
                    "topic": topic,
                    "depth": depth,
                    "partitions": partitions
                },
                "expected": res
            })
        except Exception as e:
            print(f"Warning: Random case failed: {topic} - {e}")

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"Generated {len(data)} test cases in {output_file}")

if __name__ == "__main__":
    print(generate_golden_data())
