FROM jupyter/pyspark-notebook:python-3.8.8

WORKDIR /home/jovyan/work

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt 

COPY src /home/jovyan/work/src
COPY test /home/jovyan/work/test
COPY main.py /home/jovyan/work/main.py