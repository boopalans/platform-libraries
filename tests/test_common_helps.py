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

Name:       test_common_helpers
Purpose:    common_helpers testcases
"""
import unittest
import logging
from .context import platformlibs

class CommonHelperTestCase(unittest.TestCase):
    """ Common helper library test cases """

    def test_flatten_dict(self):
        """flatten_dict test case """

        test_dict = {'a':[{'a1':'val_1', 'a2':'val_2', 'a3':[{'a4':'val_4'}, {'a5':'val_5'}], 'b':'val_6'}], 'c':'val_7'}

        flattened_dict = platformlibs.common_helpers.flatten_dict(test_dict)

        logging.debug("Test common_helper.flatten_dict")

        # validate flattened item values
        self.assertEqual(flattened_dict['a-a3-a5'], 'val_5')
        logging.debug(flattened_dict['a-a3-a5'])

        self.assertEqual(flattened_dict['a-a3-a4'], 'val_4')
        logging.debug(flattened_dict['a-a3-a4'])

        self.assertEqual(flattened_dict['a-b'], 'val_6')
        logging.debug(flattened_dict['a-b'])

        self.assertEqual(flattened_dict['a-a1'], 'val_1')
        logging.debug(flattened_dict['a-a1'])

        self.assertEqual(flattened_dict['a-a2'], 'val_2')
        logging.debug(flattened_dict['a-a2'])
