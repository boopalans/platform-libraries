#!/bin/bash
#
# Please check pnda-build/ for the build products

export VERSION=${1//-/_}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

function code_quality_error {
    echo "${1}"
}

echo -n "SPARK_HOME: "
if [[ $($SPARK_HOME/bin/spark-submit --version 2>&1) != *"version 1.6"* ]]; then
    error
else
    echo "OK"
fi

echo -n "Code quality: "
PYLINTOUT=$(find . -type f -name '*.py' | grep -vi __init__ | xargs pylint)
SCORE=$(echo ${PYLINTOUT} | grep -Po '(?<=rated at ).*?(?=/10)')
echo ${SCORE}
if [[ $(bc <<< "${SCORE} > 9") == 0 ]]; then
    code_quality_error "${PYLINTOUT}"
fi

export SPARK_VERSION='1.6.0'
export HADOOP_VERSION='2.6'
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

nosetests tests
[[ $? -ne 0 ]] && exit -1

mkdir -p pnda-build
python setup.py bdist_egg
mv dist/platformlibs-${VERSION}-py2.7.egg pnda-build/
sha512sum pnda-build/platformlibs-${VERSION}-py2.7.egg > pnda-build/platformlibs-${VERSION}-py2.7.egg.sha512.txt
