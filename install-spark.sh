#!/usr/bin/env bash
set -e

SPARK_VERSION='1.5.0'

HADOOP_VERSION='2.6'

if [ -z "$SPARK_HOME" ]
then
  echo "SPARK_HOME is unset; install spark now. "
  curl http://apache.mirror.gtcomm.net/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
  cwd=$(pwd)
  cd /tmp && tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
  cd $cwd
  export SPARK_HOME=/tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
else
  echo SPARK_HOME is already set as $SPARK_HOME
fi

