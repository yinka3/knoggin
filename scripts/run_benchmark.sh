#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./run_benchmark.sh <dataset> [start] [end]"
    echo "  dataset: 'tempo' or 'single' or 'multi"
    echo "  start: starting index (default 0)"
    echo "  end: ending index (default 4)"
    echo ""
    echo "Examples:"
    echo "  ./run_benchmark.sh multi         # Run multi-session 0-4"
    echo "  ./run_benchmark.sh single 0 9    # Run single-user 0-9"
    exit 1
fi

dataset=$1
start=${2:-0}
end=${3:-4}

timings_file=$(mktemp)

echo "Running $dataset dataset, instances $start to $end"
echo ""

for i in $(seq $start $end); do
    echo "=========================================="
    echo "Instance $i / $end ($dataset)"
    echo "=========================================="
    
    ./clean.sh
    sleep 7
    
    start_time=$(date +%s)
    
    cd benchmark
    uv run test/run_eval.py $dataset $i
    cd ..
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "$i $duration" >> "$timings_file"
    
    echo "Instance $i complete in ${duration}s"
    echo ""
done

echo "All instances finished"

# Aggregate results
python3 - "$timings_file" "$dataset" "$start" "$end" << 'EOF'
import json
import sys
from pathlib import Path

timings_file = sys.argv[1]
dataset = sys.argv[2]
start = int(sys.argv[3])
end = int(sys.argv[4])

timings = {}
with open(timings_file) as f:
    for line in f:
        idx, dur = line.strip().split()
        timings[int(idx)] = int(dur)

results = []
for i in range(start, end + 1):
    path = Path(f"benchmark/test/eval_result_{dataset}_{i}.json")
    if path.exists():
        with open(path) as f:
            data = json.load(f)
            data["duration_seconds"] = timings.get(i)
            results.append(data)

output = {
    "dataset": dataset,
    "range": f"{start}-{end}",
    "total": len(results),
    "instances": results
}

output_file = Path(f"benchmark/test/benchmark_{dataset}_{start}_{end}.json")
with open(output_file, "w") as f:
    json.dump(output, f, indent=2)

print(f"Aggregated {len(results)} results to {output_file}")
EOF

rm "$timings_file"