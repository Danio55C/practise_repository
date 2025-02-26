FROM python:3.9

WORKDIR /usr/app

COPY requirements.txt ./


RUN pip install --no-cache-dir -r requirements.txt

COPY . . 

RUN mkdir -p /usr/app/logs && chmod -R 777 /usr/app/logs

#CMD ["python", "main.py"]