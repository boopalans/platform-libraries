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

Name:       context
Purpose:    set up pyspark context
"""

import sys
import os
import unittest
# pylint: disable=unused-import
import platformlibs

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#Add libs to the PYTHONPATH
try:
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python", "lib", "py4j-0.8.2.1-src.zip"))
except KeyError:
    print "SPARK_HOME not found."
    sys.exit(1)

# pylint: disable=wrong-import-position
from pyspark import SparkContext, SparkConf


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        # initialize spark context
        spark_conf = SparkConf()
        spark_conf.set("spark.app.name", "platformlibs-test-cases")
        spark_conf.set("spark.cores.max", "1")
        spark_conf.set("spark.master", 'local[2]')
        self.spark_context = SparkContext(conf=spark_conf)

    def tearDown(self):
        self.spark_context.stop()
