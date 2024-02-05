FROM ubuntu:23.10
RUN apt-get update && apt-get install -y unzip python3 python3-pip
RUN pip3 install pandas===2.1.0 --break-system-packages

COPY hello.py /var/run/hello.py
CMD ["python3", "/var/run/hello.py"]
