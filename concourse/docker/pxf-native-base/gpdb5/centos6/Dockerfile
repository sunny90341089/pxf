ARG BASE_IMAGE=gpdb-pxf-dev/gpdb5-centos6-test-pxf:latest

FROM ${BASE_IMAGE}

RUN yum install -y -d1 \
    libzstd \
    libzstd-devel

######
# Install Google Protobuf 2.5.0 (2.6.0 ships with Xenial)
######
RUN mkdir -p /opt/protobuf-src \
    && curl -L -s -S \
      https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz \
      -o /opt/protobuf.tar.gz \
    && tar xzf /opt/protobuf.tar.gz --strip-components 1 -C /opt/protobuf-src \
    && cd /opt/protobuf-src \
    && ./configure --prefix=/opt/protobuf \
    && make install \
    && cd /root \
    && rm -rf /opt/protobuf-src
ENV PROTOBUF_HOME /opt/protobuf
ENV PATH "${PATH}:/opt/protobuf/bin"
