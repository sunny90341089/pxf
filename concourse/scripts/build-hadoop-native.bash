#!/usr/bin/env bash

set -eox pipefail

CWDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "${CWDIR}/pxf_common.bash"
source ~/.pxfrc

if [[ -f hadoop-build-dependencies/hadoop-native-build-dependencies.tar.gz ]]; then
	tar -xzf "hadoop-build-dependencies/hadoop-native-build-dependencies.tar.gz" -C ~root
fi

pushd hadoop_src
mvn package \
	-Pdist,native \
	-DskipTests \
	-Drequire.zlib \
	-Drequire.snappy \
	-Drequire.zstd \
	-Drequire.lz4 \
	-Drequire.bzip2 \
	-Dtar
popd

cp hadoop_src/hadoop-dist/target/hadoop-*.tar.gz dist

# tar .m2 cache
tar -czf dist/hadoop-native-build-dependencies.tar.gz -C ~ .m2
