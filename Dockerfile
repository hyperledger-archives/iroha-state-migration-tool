FROM python:3.7-alpine

COPY requirements.txt /requirements.txt
RUN \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 pip install  -r /requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

COPY . /app
WORKDIR /app
ENTRYPOINT ["python3", "state_migration.py"]