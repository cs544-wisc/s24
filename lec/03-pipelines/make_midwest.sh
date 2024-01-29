#! /usr/bin/bash

cat data/stations.txt | grep " WI " > midwest.txt
cat data/stations.txt | grep " IL " >> midwest.txt
