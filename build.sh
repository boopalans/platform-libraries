#!/bin/bash
#
# Please check pnda-build/ for the build products

export VERSION=${1//-/_}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

echo -n "SPARK_HOME: "
if [[ $($SPARK_HOME/bin/spark-submit --version 2>&1) != *"version 1.5.0"* ]]; then
    error
else
    echo "OK"
fi

mkdir -p pnda-build
SPARK_VERSION='1.5.0'
export HADOOP_VERSION='2.6'
python setup.py bdist_egg
mv dist/platformlibs-${VERSION}-py2.7.egg pnda-build/
sha512sum pnda-build/platformlibs-${VERSION}-py2.7.egg > pnda-build/platformlibs-${VERSION}-py2.7.egg.sha512.txt
