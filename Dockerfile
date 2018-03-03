FROM centos:7

MAINTAINER Wu Tao <wutao1@xiaomi.com>

RUN yum -y install gcc gcc-c++ automake autoconf libtool make cmake git file wget && \
    yum -y install openssl-devel boost-devel libaio-devel snappy-devel bzip2-devel readline-devel zlib zlib-devel patch

ADD . /pegasus

RUN cd /pegasus && \
    ./run.sh build -c

RUN cd /pegasus && \
    cp -r src/builder/bin/pegasus_server /pegasus_server && \
    rm -rf /pegasus

EXPOSE 34801

ENTRYPOINT ["/pegasus-server"]
