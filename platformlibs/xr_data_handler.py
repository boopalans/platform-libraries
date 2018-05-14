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

Name:       xr_data_handler
Purpose:    XR Telemetry Data Handler Implementation
"""
import re
import json
from platformlibs.json_data_handler import JsonDataHandler
from platformlibs.common_helpers import flatten_dict

class XrDataHandler(JsonDataHandler):
    '''
    Sample Telemetry data handler library
    '''
    def __init__(self,
                 spark_context,
                 datasource,
                 path,
                 isTopic=False):
        '''
        Constructor
        :param sc:
        :param cluster_name:
        :param manager_hostname:
        '''
        JsonDataHandler.__init__(self,
                                 spark_context,
                                 datasource,
                                 path,
                                 isTopic)

    @staticmethod
    def preprocess(data_dict):
        '''
        pre-processing telemetry data
        :param data_dict:
        '''
        raw_data_str = data_dict.pop('rawdata').decode('utf-8')
        raw_data_str = re.sub(r'"{"', '{"', raw_data_str)
        raw_data_str = re.sub(r'}"}', '}}', raw_data_str)
        raw_data_dict = json.loads(raw_data_str)
        data_dict['rawdata'] = flatten_dict(raw_data_dict)
        return data_dict

    def list_metric_ids(self, limit=-1, filters=None):
        """ return list of metric statistics """
        #embedded function declaration
        def get_count(item):
            return item[1]

        t_rdd = self.rdd
        m_filter = None
        m_type_filter = None

        if filters:
            # extract metric list filter if exists
            m_filter = filters.pop('metrics', None)
            m_type_filter = filters.pop('metric_type', None)
            for key in filters:
                t_rdd = t_rdd.filter(lambda x: x['rawdata'][key] in filters[key])
            t_rdd = t_rdd.map(lambda x: {i:x['rawdata'][i] for i in x['rawdata'] if i not in filters})
        else:
            t_rdd = t_rdd.map(lambda x: x['rawdata'])

        t_rdd = t_rdd.flatMap(lambda x: x.keys()) \
                    .map(lambda x: (x, 1)) \
                    .reduceByKey(lambda x, y: x + y)

        if m_filter:
            t_rdd = t_rdd.filter(lambda x: x[0] in m_filter)

        if m_type_filter:
            if m_type_filter is 'ipsla':
                t_rdd = t_rdd.filter(lambda x: ('ipsla' in x))
            if m_type_filter is 'mpls':
                t_rdd = t_rdd.filter(lambda x: ('mpls' in x))
            if m_type_filter is 'infra':
                t_rdd = t_rdd.filter(lambda x: ('ipsla' not in x)) \
                             .filter(lambda x: ('mpls' not in x))

        stats = t_rdd.collect()
        stats = sorted(stats, key=get_count)
        if limit > 0:
            stats = stats[0:limit]
        return stats
