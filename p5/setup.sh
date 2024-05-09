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

# installs
sudo apt install -y unzip

# files download
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/nbutils.py -O nbutils.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/tester.py -O tester.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p5/autograde.py -O autograde.py
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p5/datanode.Dockerfile -O datanode.Dockerfile
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p5/docker-compose.yml -O docker-compose.yml
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p5/namenode.Dockerfile -O namenode.Dockerfile
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p5/notebook.Dockerfile -O notebook.Dockerfile
wget https://raw.githubusercontent.com/cs544-wisc/s24/main/p5/p5-base.Dockerfile -O p5-base.Dockerfile


# data download
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip -O hdma-wi-2021.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/arid2017_to_lei_xref_csv.zip -O arid2017_to_lei_xref_csv.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/code_sheets.zip -O code_sheets.zip

# unzipping
mkdir -p nb/data
unzip -o hdma-wi-2021.zip -d nb/data
unzip -o arid2017_to_lei_xref_csv.zip -d nb/data
unzip -o code_sheets.zip -d nb/data

# cleanup
rm hdma-wi-2021.zip
rm arid2017_to_lei_xref_csv.zip
rm code_sheets.zip

echo -e "${GREEN}Success: Setup finished!${NC}"
