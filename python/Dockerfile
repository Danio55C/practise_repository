FROM python:3.9

RUN pip install mysql-connector-python
RUN pip install pymemcache
RUN pip install elasticsearch
RUN pip install kafka-python
RUN pip install loguru
RUN pip install pandas
RUN pip install matplotlib
RUN pip install dataframe-image
RUN pip install numpy
RUN pip install seaborn



WORKDIR /usr/app/src

COPY main.py ./