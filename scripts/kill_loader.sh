#!/bin/bash
# Kill running ldbc_loader.py processes
PIDS=$(pgrep -f ldbc_loader.py)
if [ -z "$PIDS" ]; then
    echo "No ldbc_loader.py process found."
    exit 0
fi
for pid in $PIDS; do
    echo "Killing $pid ..."
    kill "$pid"
done
echo "Done."
