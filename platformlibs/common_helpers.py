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

Name:       common_helpers
Purpose:    Utility library that defines common helper functions
"""
import requests
from cm_api.api_client import ApiResource

def flatten_dict(input_d, result=None):
    '''
    Fatten a dictionary object
    example: the following json document:
    {'a':[{'a1':'val_1',
           'a2':'val_2',
           'a3':[{'a4':'val_4'},
           {'a5':'val_5'}],
     'b':'val_6'}],
     'c':'val_7'}
    will have the following output:
    {'a-a3-a5': 'val_5',
     'a-a3-a4': 'val_4',
     'a-b': 'val_6',
     'c': 'val_7',
     'a-a1': 'val_1',
     'a-a2': 'val_2'}
    '''

    if result is None:
        result = {}
    for key in input_d:
        value = input_d[key]
        if isinstance(value, dict):
            new_val = {}
            for key_in in value:
                new_val[".".join([key, key_in])] = value[key_in]
            flatten_dict(new_val, result)
        elif isinstance(value, (list, tuple)):
            for index_b, element in enumerate(value):
                if isinstance(element, dict):
                    new_val = {}
                    for key_in in element:
                        new_val["-".join([key, key_in])] = value[index_b][key_in]
                    for _ in new_val:
                        flatten_dict(new_val, result)
        else:
            result[key] = value
    return result


def connect_cm(cm_api, cm_username, cm_password):
    '''
    connnect to cloudera manager endpoint
    Args:
        - cm_username: cloudera manager login user
        - cm_password: cloudera manager login password
    '''
    api = ApiResource(
        cm_api,
        version=6,
        username=cm_username,
        password=cm_password)
    return api

def ambari_request(ambari, uri):
    hadoop_manager_ip = ambari[0]
    hadoop_manager_username = ambari[1]
    hadoop_manager_password = ambari[2]
    if uri.startswith("http"):
        full_uri = uri
    else:
        full_uri = 'http://%s:8080/api/v1%s' % (hadoop_manager_ip, uri)

    headers = {'X-Requested-By': hadoop_manager_username}
    auth = (hadoop_manager_username, hadoop_manager_password)
    return requests.get(full_uri, auth=auth, headers=headers).json()

def get_hdfs_hdp(ambari, cluster_name):
    core_site = ambari_request(ambari, '/clusters/%s?fields=Clusters/desired_configs/core-site' % cluster_name)
    config_version = core_site['Clusters']['desired_configs']['core-site']['tag']
    core_site_config = ambari_request(ambari, '/clusters/%s/configurations/?type=core-site&tag=%s' % (cluster_name, config_version))
    return core_site_config['items'][0]['properties']['fs.defaultFS']

def get_name_service(cm_host, cluster_name, service_name, user_name='admin', password='admin'):
    '''
    get name service
    Args:
        - cm_host: hadoop manager host name
        - service_name: service name
        - cm_username: hadoop manager login user
        - cm_password: hadoop manager login password
    '''
    request_url = 'http://%s:7180/api/v11/clusters/%s/services/%s/nameservices' % (cm_host, cluster_name, service_name)
    result = requests.get(request_url, auth=(user_name, password))
    name_service = None
    if result.status_code == 200:
        response = result.json()
        if 'items' in response:
            name_service = response['items'][0]['name']
    return name_service

def get_hdfs_uri(cm_host, cm_user, cm_pass, hadoop_distro):
    '''
    return hdfs root uri
    args:
        - cm_host: hadoop manager host name
        - cm_username: hadoop manager login user
        - cm_password: hadoop manager login password
        - hadoop_distro: 'CDH' or 'HDP'
    '''
    hdfs_uri = ''

    if hadoop_distro == 'CDH':
        api = connect_cm(cm_host, cm_user, cm_pass)

        for cluster_detail in api.get_all_clusters():
            cluster_name = cluster_detail.name
            break

        cluster = api.get_cluster(cluster_name)
        for service in cluster.get_all_services():
            if service.type == "HDFS":
                name_service = get_name_service(cm_host, cluster_name, service.name, cm_user, cm_pass)
                if name_service:
                    hdfs_uri = 'hdfs://%s' % name_service
                for role in service.get_all_roles():
                    if not name_service and role.type == "NAMENODE":
                        hdfs_uri = 'hdfs://%s:8020' % api.get_host(role.hostRef.hostId).ipAddress
    else:
        ambari = (cm_host, cm_user, cm_pass)
        cluster_name = ambari_request(ambari, '/clusters')['items'][0]['Clusters']['cluster_name']
        hdfs_uri = get_hdfs_hdp(ambari, cluster_name)

    return hdfs_uri
