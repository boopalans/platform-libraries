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

Name:       test_xr_data_handler
Purpose:    XR telemetry data handler test cases
"""

import logging
import mock
from .context import platformlibs
from .context import SparkTestCase

# pylint: disable=invalid-name
t_input = [{u'timestamp': 1439535267000,
            u'rawdata': '''{"identifier":"small_e",
                          "path":"%path",
                          "type":"counters",
                          "content":"{"counters":{"PacketsReceived":0,
                                                  "BytesReceived":0,
                                                  "PacketsSent":0,
                                                  "BytesSent":0,
                                                  "MulticastPacketsReceived":0,
                                                  "BroadcastPacketsReceived":0,
                                                  "MulticastPacketsSent":0,
                                                  "BroadcastPacketsSent":0,
                                                  "OutputDrops":0,
                                                  "OutputQueueDrops":0,
                                                  "InputDrops":0,
                                                  "InputQueueDrops":0,
                                                  "RuntPacketsReceived":0,
                                                  "GiantPacketsReceived":0,
                                                  "ThrottledPacketsReceived":0,
                                                  "ParityPacketsReceived":0,
                                                  "UnknownProtocolPacketsReceived":0,
                                                  "InputErrors":0,
                                                  "CRCErrors":0,
                                                  "InputOverruns":0,
                                                  "FramingErrorsReceived":0,
                                                  "InputIgnoredPackets":0,
                                                  "InputAborts":0,
                                                  "OutputErrors":0,
                                                  "OutputUnderruns":0,
                                                  "OutputBufferFailures":0,
                                                  "OutputBuffersSwappedOut":0,
                                                  "Applique":0,"Resets":0,
                                                  "CarrierTransitions":0,
                                                  "AvailabilityFlag":0,
                                                  "LastDataTime":1439535262,
                                                  "SecondsSinceLastClearCounters":0,
                                                  "LastDiscontinuityTime":1439332412,
                                                  "SecondsSincePacketReceived":4294967295,
                                                  "SecondsSincePacketSent":4294967295},
                                                  "key":{"InterfaceName":"Null0"}}"}
                        ''',
            u'source': u'telemetry'}]

class XrDataHandlerTestCase(SparkTestCase):

    @mock.patch('platformlibs.xr_data_handler.XrDataHandler.rdd')
    def test_list_metric_ids(self, mock_rdd):
        """ test list_metric_ids """
        t_rdd = self.spark_context.parallelize(t_input)
        preprocess = platformlibs.xr_data_handler.XrDataHandler.preprocess
        test_rdd = t_rdd.map(lambda x: preprocess(x))
        # set up the mock
        # pylint: disable=protected-access
        mock_rdd = mock.PropertyMock(return_value=test_rdd)
        handler = platformlibs.xr_data_handler.XrDataHandler(self.spark_context, "mocked source", "mocked path")
        type(handler).rdd = mock_rdd
        # test list metrics without limits and filters
        expected_result = [(u'counters.InputErrors', 1),
                           (u'counters.ParityPacketsReceived', 1),
                           (u'counters.OutputErrors', 1),
                           (u'counters.MulticastPacketsReceived', 1),
                           (u'counters.SecondsSincePacketReceived', 1),
                           (u'counters.InputAborts', 1), (u'counters.CarrierTransitions', 1),
                           (u'counters.SecondsSinceLastClearCounters', 1),
                           (u'counters.InputQueueDrops', 1),
                           (u'counters.LastDiscontinuityTime', 1),
                           (u'counters.BytesSent', 1),
                           (u'counters.PacketsSent', 1),
                           (u'counters.Resets', 1),
                           (u'key.InterfaceName', 1),
                           (u'counters.OutputQueueDrops', 1),
                           (u'counters.ThrottledPacketsReceived', 1),
                           (u'counters.RuntPacketsReceived', 1),
                           (u'counters.InputIgnoredPackets', 1),
                           (u'counters.OutputUnderruns', 1),
                           (u'counters.InputDrops', 1),
                           (u'counters.CRCErrors', 1),
                           (u'counters.OutputDrops', 1),
                           (u'counters.UnknownProtocolPacketsReceived', 1),
                           (u'counters.BroadcastPacketsSent', 1),
                           (u'counters.OutputBufferFailures', 1),
                           (u'counters.MulticastPacketsSent', 1),
                           (u'counters.InputOverruns', 1),
                           (u'counters.GiantPacketsReceived', 1),
                           (u'counters.FramingErrorsReceived', 1),
                           (u'counters.OutputBuffersSwappedOut', 1),
                           (u'counters.Applique', 1),
                           (u'counters.PacketsReceived', 1),
                           (u'counters.SecondsSincePacketSent', 1),
                           (u'counters.BytesReceived', 1),
                           (u'counters.LastDataTime', 1),
                           (u'counters.AvailabilityFlag', 1),
                           (u'counters.BroadcastPacketsReceived', 1)]

        result = handler.list_metric_ids()
        logging.debug(result)
        self.assertEqual(result, expected_result)

        # test list metrics filtered by metric type
        filters = {'metric_type':'infra'}
        result = handler.list_metric_ids(filters=filters)
        logging.debug(result)
        self.assertEqual(result, expected_result)

        filters = {'metric_type':'ipsla'}
        result = handler.list_metric_ids(filters=filters)
        logging.debug(result)
        self.assertEqual(len(result), 0)

        filters = {'metric_type':'mpls'}
        result = handler.list_metric_ids(filters=filters)
        logging.debug(result)
        self.assertEqual(len(result), 0)
