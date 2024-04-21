#!/bin/bash

# check if a github repo
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

if [ -d .git ]; then
    echo -e "${GREEN}Ok: This is a git repo.${NC}"
else
    echo -e "${RED}Error: Not a git repo.${NC}"
    echo "Setup exited without downloading any files."
    echo "Create a repository through the Github Classroom link."
    exit 1
fi;

# files download
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p8/autograde.py -O autograde.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p8/.gitignore -O .gitignore
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/tester.py -O tester.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/nbutils.py -O nbutils.py

# finished
echo -e "${GREEN}Success: Setup finished!${NC}"