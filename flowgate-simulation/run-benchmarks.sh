#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mvn -pl flowgate-simulation -am clean install
cd flowgate-simulation
mvn exec:java -Dexec.args="../benchmark-results/latest"
python3 -m pip install -r requirements.txt
python3 scripts/generate_report.py --input ../benchmark-results/latest --output ../benchmark-results/latest/report
