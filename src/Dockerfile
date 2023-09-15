FROM python

COPY filter.py /


RUN pip install --upgrade pip && \
    pip install boto3 && \
    pip install boto && \
    pip install pandas && \
    pip install pyarrow && \
    pip install s3fs

RUN pwd
RUN ls

CMD ["python", "filer.py"]
