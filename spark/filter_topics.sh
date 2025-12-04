#!/usr/bin/env bash
set -euo pipefail

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required. Install Python 3 and try again."
  exit 1
fi

if [ $# -lt 1 ]; then
  echo "Usage: $0 <mother.csv> [output-dir]"
  echo "Example: $0 mother.csv ./out"
  exit 1
fi

INPUT="$1"
OUTDIR="${2:-.}"
mkdir -p "$OUTDIR"

python3 - "$INPUT" "$OUTDIR" <<'PY'
import csv, sys, os

input_path = sys.argv[1]
outdir = sys.argv[2]

# Topics to group into the single laravel CSV
laravel_topics = {"demo-laravel-logs", "demo-laravel-traces", "demo-laravel-metrics"}
# Single-topic output
minikube_topic = "demo-minikube-node_metrics"

laravel_path = os.path.join(outdir, "demo-laravel.csv")
minikube_path = os.path.join(outdir, "demo-minikube-node_metrics.csv")

count_total = 0
count_laravel = 0
count_minikube = 0

try:
    with open(input_path, newline='', encoding='utf-8-sig') as inf:
        reader = csv.reader(inf)
        try:
            header = next(reader)
        except StopIteration:
            print("Input CSV is empty. No output files created.")
            sys.exit(0)

        # find 'topic' column index (case-insensitive)
        topic_idx = None
        for i, h in enumerate(header):
            if h is None:
                continue
            if h.strip().lower() == 'topic':
                topic_idx = i
                break

        if topic_idx is None:
            sys.stderr.write("ERROR: 'topic' column not found in header.\n")
            sys.stderr.write("Header columns: {}\n".format(header))
            sys.exit(2)

        with open(laravel_path, 'w', newline='', encoding='utf-8') as lf, \
             open(minikube_path, 'w', newline='', encoding='utf-8') as mf:
            lout = csv.writer(lf)
            mout = csv.writer(mf)
            # write header to both outputs
            lout.writerow(header)
            mout.writerow(header)

            for row in reader:
                count_total += 1
                if topic_idx >= len(row):
                    # row missing topic column — skip
                    continue
                topic_val = row[topic_idx].strip()
                if topic_val in laravel_topics:
                    lout.writerow(row)
                    count_laravel += 1
                if topic_val == minikube_topic:
                    mout.writerow(row)
                    count_minikube += 1

    print(f"Input: {input_path}")
    print(f"Rows processed (excluding header): {count_total}")
    print(f"Created: {laravel_path} (rows: {count_laravel})")
    print(f"Created: {minikube_path} (rows: {count_minikube})")

except FileNotFoundError:
    sys.stderr.write(f"ERROR: Input file not found: {input_path}\n")
    sys.exit(3)
PY


# run command: will save on on the out directory
# ./filter_topics.sh ./exports/kafka_logs_export_20250917_105635.csv ./out