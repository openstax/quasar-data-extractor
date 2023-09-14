FROM python

COPY test_load.py /


RUN pip install --upgrade pip && \
    pip install boto3 && \
    pip install boto && \
    pip install pyarrow && \
    pip install s3fs

RUN pwd
RUN ls

CMD ["python", "test_load.py"]
