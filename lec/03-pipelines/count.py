#! /usr/bin/python3

count = 0

from pathlib import Path, PurePath
import time

# absolute path: /home/meenakshisyamkumar/data/stations.txt
# relative path: data/stations.txt
# Problem: Code wouldn't work on Windows!

with open(PurePath(Path("data"), Path("sample")) / "stations.txt") as f:
    for line in f:
        count = count + 1
        time.sleep(1) # sleep for 1 second
        print(count)

print(count)

