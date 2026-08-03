#!/bin/bash
JOB_ID=$1
echo "Waiting for job $JOB_ID to complete..."
while true; do
  # Check sacct first
  STATE=$(sacct --format=State --noheader -j $JOB_ID 2>/dev/null | head -1 | tr -d ' ')
  # If empty, check squeue
  if [[ -z "$STATE" ]]; then
     STATE=$(squeue -j $JOB_ID --format="%T" --noheader 2>/dev/null | tr -d ' ')
  fi
  
  if [[ "$STATE" == "COMPLETED" || "$STATE" == "FAILED" || "$STATE" == "CANCELLED"* || "$STATE" == "TIMEOUT" || "$STATE" == *"OUT_OF_ME"* ]]; then
    echo "Job $JOB_ID finished with state: $STATE"
    echo "--- Last 50 lines of log ---"
    cat logs/*_${JOB_ID}.log 2>/dev/null | tail -n 50
    break
  fi
  sleep 120
done
