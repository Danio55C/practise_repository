FROM python:3.9

RUN pip install mysql-connector-python
RUN pip install pymemcache
RUN pip install elasticsearch
RUN pip install kafka-python
RUN pip install loguru


WORKDIR /usr/app/src

COPY main.py ./