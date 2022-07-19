FROM python:3-alpine
USER root
RUN adduser -D muser
RUN pip install kafka-python
ADD classified_producer.py /home/muser
RUN chmod a+x /home/muser/classified_producer.py
USER muser
ENTRYPOINT ["/home/muser/classified_producer.py"]