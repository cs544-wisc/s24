#!/bin/bash
sudo apt-get update
sudo apt install -y python3 python3-pip
pip3 install pandas docker 

for filename in "tester.py" "p4/autograde.py" "p4/docker-compose.yml" "p4/hdfs.Dockerfile" "p4/notebook.Dockerfile" "nbutils.py"
do
    wget https://raw.githubusercontent.com/cs544-wisc/s24/main/$filename -O $(basename $filename)
done
