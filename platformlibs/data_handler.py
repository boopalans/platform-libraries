"""
Copyright (c) 2016 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.
Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.

Name:       data_handler
Purpose:    Abstract DataHandler class
"""

from abc import ABCMeta
from abc import abstractmethod
from platformlibs.config_helper import read_config
from platformlibs.common_helpers import get_hdfs_uri

class DataHandler(object):
    """
    Abstract data handler class
    """
    __metaclass__ = ABCMeta

    def __init__(self,
                 spark_context,
                 datasource,
                 path,
                 isTopic=False):
        """ Constructor """
        self.spark_context = spark_context
        self.datasource = datasource
        self.is_topic = isTopic
        self.path = path
        self._rdd = None
        self._hdfs_root_uri = None

    @staticmethod
    def preprocess(raw_data):
        """ return raw rdd """
        return raw_data

    @abstractmethod
    def list_metric_ids(self, limit=-1, filters=None):
        """ return list of metrics """
        pass

    @abstractmethod
    def execute_query(self, filters=None):
        """ return time-series measurements of a metric"""
        pass

    @property
    def hdfs_root_uri(self):
        """ calculate hdfs root uri """
        if self._hdfs_root_uri:
            return self._hdfs_root_uri
        cm_conf = read_config('/etc/platformlibs/platformlibs.ini')
        self._hdfs_root_uri = get_hdfs_uri(cm_conf['cm_host'], cm_conf['cm_user'], cm_conf['cm_pass'], cm_conf['hadoop_distro'])
        return self._hdfs_root_uri

    @property
    def rdd(self):
        """ return raw data as rdd
        Args:
            - datasource: data source name
            - path: the relative path to data source directory
        """
        if self._rdd:
            return self._rdd

        root = ('{}/user/pnda/PNDA_datasets/datasets/topic={}/{}' \
            if self.is_topic else \
            '{}/user/pnda/PNDA_datasets/datasets/source={}/{}') \
            .format(self.hdfs_root_uri,
                    self.datasource,
                    self.path)
        conf = {
            "mapreduce.input.fileinputformat.input.dir.recursive": "true"
        }
        data_rdd = self.spark_context \
                  .newAPIHadoopFile(
                      root,
                      "org.apache.avro.mapreduce.AvroKeyInputFormat",
                      "org.apache.avro.mapred.AvroKey",
                      "org.apache.hadoop.io.NullWritable",
                      keyConverter=
                      'org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter',
                      conf=conf)
        preprocess = self.preprocess
        self._rdd = data_rdd.map(lambda x: preprocess(x[0]))
        return self._rdd
