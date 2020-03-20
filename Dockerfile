FROM python:3.8-alpine

RUN apk update && apk add gcc libc-dev libffi-dev openssl-dev python3-dev
RUN apk add --update tzdata
ENV TZ=Asia/Calcutta

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
RUN chmod 755 entrypoint.sh
RUN /usr/bin/crontab crontab.txt


CMD ["/usr/src/app/entrypoint.sh"]
