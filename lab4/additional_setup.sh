#!/bin/bash
apt-get update && apt-get install -y python3-distutils python3-apt
wget https://bootstrap.pypa.io/pip/3.6/get-pip.py
python3 get-pip.py
pip install jupyter
pip install kazoo
