FROM frolvlad/alpine-glibc:alpine-3.7

RUN apk add --no-cache bash

RUN mkdir -p /plugin/logs

ADD ./conf /plugin/conf

ADD ./humpback-logdriver /plugin/

WORKDIR /plugin

CMD ["./humpback-logdriver"]
