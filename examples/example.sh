#!/bin/bash
set -e

echo "HELLO FROM BASH SCRIPT:"

echo "Current directory: $(pwd)"

which python

echo MYENVVAR: $MYENVVAR

mkdir -p results
printf 'Example result\nMYENVVAR: %s\n' "${MYENVVAR:-}" > results/result.txt
echo "Saved results/result.txt"

echo "Done"
