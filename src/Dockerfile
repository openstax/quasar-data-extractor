FROM python:3.11

COPY filter.py /


RUN pip install --upgrade pip && \
    pip install boto3 && \
    pip install boto && \
    pip install pandas && \
    pip install pyarrow && \
    pip install requests && \
    pip install s3fs

RUN pwd
RUN ls

CMD ["python", "filter.py"]
