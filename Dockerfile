FROM frolvlad/alpine-glibc:alpine-3.7

RUN apk add --no-cache bash

RUN mkdir -p /plugin/logs

RUN mkdir -p /run/docker/logging/meta_confs

RUN mkdir -p /run/docker/logging/blocks

ADD ./conf /plugin/conf

ADD ./humpback-logdriver /plugin/

WORKDIR /plugin

CMD ["./humpback-logdriver"]
