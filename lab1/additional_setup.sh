#!/bin/bash
echo 'export PATH=$PATH:/opt/mapr/spark/spark-3.2.0/bin' > /root/.bash_profile
source /root/.bash_profile

apt-get update && apt-get install -y python3-distutils python3-apt
wget https://bootstrap.pypa.io/pip/3.6/get-pip.py
python3 get-pip.py
pip install jupyter
pip install pyspark
