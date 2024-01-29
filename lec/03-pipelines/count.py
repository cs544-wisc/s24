#!/usr/bin/python3

count = 0

from pathlib import Path

# absolute path: /home/meenakshisyamkumar/data/stations.txt
# relative path: data/stations.txt
# Problem: Code wouldn't work on Windows!

with open(Path("data") / "stations.txt") as f:
    for line in f:
        count = count + 1

print(count)
