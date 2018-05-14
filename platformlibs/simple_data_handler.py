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

Name:       simple_data_handler
Purpose:    simple data handler implementation
"""

from platformlibs.data_handler import DataHandler

class SimpleDataHandler(DataHandler):
    '''
    Simple data handler for loading HDFS data as rdd only.
    '''
    def __init__(self,
                 spark_context,
                 datasource,
                 path,
                 isTopic=False):
        '''
        Constructor
        :param sc:
        :param datasource:
        :param topic:
        :param path:
        :param cluster_name:
        :param manager_hostname:
        '''
        DataHandler.__init__(self,
                             spark_context,
                             datasource,
                             path,
                             isTopic)

    def list_host_ips(self):
        """ not supported """
        raise Exception('not implemented - use JsonDataHandler or provide custom implementations')

    def list_metric_ids(self, limit=-1, filters=None):
        """ not supported """
        raise Exception('not implemented - use JsonDataHandler or provide custom implementations')

    def execute_query(self, filters=None):
        """ not supported """
        raise Exception('not implemented - use JsonDataHandler or provide custom implementations')
