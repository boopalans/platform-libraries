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

Name:       test_json_data_handler
Purpose:    Json data handler test cases
"""
import logging
import mock
from .context import platformlibs
from .context import SparkTestCase

# pylint: disable=invalid-name
t_input = [{'timestamp': 1446426784000,
            'rawdata': {"version":"5", "in_pkts":"4", "in_bytes":"240"},
            'source': 'netflow'},
           {'timestamp': 1446426785000,
            'rawdata': {"version":"5", "in_pkts":"6", "in_bytes":"220"},
            'source': 'netflow'},
           {'timestamp': 1446436817000,
            'rawdata': {"version":"5", "in_pkts":"4", "in_bytes":"181"},
            'source': 'netflow'}]

class JsonDataHandlerTestCase(SparkTestCase):

    @mock.patch('platformlibs.json_data_handler.JsonDataHandler.rdd')
    def test_list_metric_ids(self, mock_rdd):
        """ test list_metric_ids """
        test_rdd = self.spark_context.parallelize(t_input)

        #set up the mock
        # pylint: disable=protected-access
        mock_rdd = mock.PropertyMock(return_value=test_rdd)
        handler = platformlibs.json_data_handler.JsonDataHandler(self.spark_context, "mocked source", "mocked path")
        type(handler).rdd = mock_rdd

        #test list metrics without limits and filters
        expected_result = [('in_bytes', 3), ('in_pkts', 3), ('version', 3)]
        result = handler.list_metric_ids()
        logging.debug(result)
        self.assertEqual(result, expected_result)

        #test list metrics with limits
        expected_result = [('in_bytes', 3)]
        result = handler.list_metric_ids(limit=1)
        logging.debug(result)
        self.assertEqual(result, expected_result)

    @mock.patch('platformlibs.json_data_handler.JsonDataHandler.rdd')
    def test_execute_query(self, mock_rdd):
        """ test list_metric_ids """
        test_rdd = self.spark_context.parallelize(t_input)

        #set up the mock
        # pylint: disable=protected-access
        mock_rdd = mock.PropertyMock(return_value=test_rdd)
        handler = platformlibs.json_data_handler.JsonDataHandler(self.spark_context, "mocked source", "mocked path")
        type(handler).rdd = mock_rdd

        #test query without filters
        expected_result = [('in_bytes', [(1446426784000, '240'), (1446426785000, '220'), (1446436817000, '181')]), 
                           ('in_pkts', [(1446426784000, '4'), (1446426785000, '6'), (1446436817000, '4')]), 
                           ('version', [(1446426784000, '5'), (1446426785000, '5'), (1446436817000, '5')])]
        result = handler.execute_query()
        logging.debug(result)
        self.assertEqual(result, expected_result)

        # test query with host_ips filter
        filters = {}
        # test query with additional timestamp filters
        filters['start_ts'] = 1446426784000
        filters['end_ts'] = 1446426784400
        expected_result = [('in_bytes', [(1446426784000, '240')]), 
                           ('in_pkts', [(1446426784000, '4')]), 
                           ('version', [(1446426784000, '5')])]
        result = handler.execute_query(filters=filters)
        logging.debug(result)
        self.assertEqual(result, expected_result)

        # test query with extra metrics filters
        filters['metrics'] = ['in_bytes']
        expected_result = [('in_bytes', [(1446426784000, '240')])]
        result = handler.execute_query(filters=filters)
        logging.debug(result)
        self.assertEqual(result, expected_result)
