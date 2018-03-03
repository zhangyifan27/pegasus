FROM centos:7

MAINTAINER Wu Tao <wutao1@xiaomi.com>

ADD . /pegasus

RUN cd /pegasus && \
    yum -y install gcc gcc-c++ automake autoconf libtool make cmake git file wget && \
    yum -y install openssl-devel boost-devel libaio-devel snappy-devel bzip2-devel readline-devel zlib zlib-devel patch

RUN cd /pegasus/rdsn/thirdparty &&
    ./download-thirdparty.sh

RUN cd /pegasus && \
    ./run.sh build -c && \
    cp -f src/builder/bin/pegasus-server /pegasus-server && \
    rm -rf /pegasus

EXPOSE 34801

ENTRYPOINT ["/pegasus-server"]
