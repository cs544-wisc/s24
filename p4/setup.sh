#!/bin/bash
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p4/autograde.py -O autograde.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/nbutils.py -O nbutils.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/tester.py -O tester.py

sudo apt-get update
sudo apt install python3-pip
pip3 install pandas docker