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

Name:       setup
Purpose:    platformlibs setup script
"""
import os
from setuptools import setup, find_packages

# pylint: disable=invalid-name
here = os.path.abspath(os.path.dirname(__file__))

readme = open(os.path.join(here, 'README.md')).read()

licence = open(os.path.join(here, 'LICENSE')).read()

version = '0.6.8'

if 'VERSION' in os.environ.keys():
    version = os.environ["VERSION"]

install_requires = []
setup(name='platformlibs',
      version=version,
      description="PNDA platform libraries",
      long_description=readme,
      author='PNDA team',
      author_email='pnda.team@external.cisco.com',
      url='https://cto-github.cisco.com/CTAO-Team6-Analytics/platform-libraries',
      license=licence,
      packages=find_packages(exclude=('tests')),
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires
     )
