FROM python:3.9

RUN pip install mysql-connector-python
RUN pip install pymemcache
RUN pip install elasticsearch

WORKDIR /usr/app/src

COPY main.py ./