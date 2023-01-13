#!/usr/bin/env python3

import argparse
import os
import time
from pprint import pprint

import googleapiclient.discovery
import google.auth

credentials, project = google.auth.default()
service = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)

# Lists instances
def list_instances(compute, project, zone):
    result = compute.instances().list(project = project, zone = zone).execute()
    return result['items'] if 'items' in result else None

# Create instance
def create_instance(compute, project, zone, name, bucket):
    image_response = compute.images().getFromFamily(project = 'ubuntu-os-cloud', family = 'ubuntu-2204-lts').execute()
    source_disk_image = image_response['selfLink']

    # Configure machine
    machine_type = "zones/%s/machineTypes/f1-micro" % zone
    startup_script = open(os.path.join(os.path.dirname(__file__), 'startup_script.sh'), 'r').read()
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify boot disk and source image
        'disks': 
        [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams':
                {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify Network address translation (NAT) mapping IP address to access internet
        'networkInterfaces': 
        [{
            'network': 'global/networks/default',
            'accessConfigs':
            [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Give access to instance in order to access cloud storage and logging
        'serviceAccounts': 
        [{
            'email': 'default',
            'scopes':
            [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Instance readable Metadata allows you passing configuration to instances from deployment scripts
        'metadata': 
        {
            'items': 
            [{
                # Startup script automatically executed by instance on startup
                'key': 'startup-script',
                'value': startup_script
            }, {
                'key': 'url',
                'value': image_url
            }, {
                'key': 'text',
                'value': image_caption
            }, {
                'key': 'bucket',
                'value': bucket
            }]
        }
    }

    return compute.instances().insert(
        project = project,
        zone = zone,
        body = config).execute()

# Wait for operation
def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project = project,
            zone = zone,
            operation = operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

# Create firewall rule
def create_firewall_rule(project):
    target = "allow-5000"
    firewall_body = {
        "name": target,
        "sourceRanges": 
        [
            "0.0.0.0/0"
        ],
        "targetTags":
        [
            target
        ],
        "allowed": 
        [
            {
                "IPProtocol": "tcp",
                "ports": 
                    [
                        "5000"
                    ]
            }
        ]
    }
    try:
        request = service.firewalls().insert(project = project, body = firewall_body)
        response = request.execute()
    except Exception as e:
        print('Could not create firewall rule. Check if the rule already exists')
        pass
    
    return target

def add_firewall_rule_to_compute_node(project, zone, instance, target_tag, fingerprint):
    tags_body = {
        "items": 
        [
            target_tag
        ],
        "fingerprint": fingerprint
    }
    request = service.instances().setTags(project = project, zone = zone, instance = instance, body = tags_body)
    response = request.execute()


# Run
if __name__ == '__main__':
    Project_ID = 'csci-4253-359820'
    Bucket_Name = 'csci4253-lab5-bucket'
    Instance_Name = 'csci4253-lab5-part1'
    Zone = 'us-west1-b'    

    operation = create_instance(service, Project_ID, Zone, Instance_Name, Bucket_Name)
    wait_for_operation(service, Project_ID, Zone, operation['name'])
    print("Your running instances are:")
    network_target = create_firewall_rule(Project_ID)
    instances = list_instances(service, Project_ID, Zone)
    if instances:
        for instance in instances:
            add_firewall_rule_to_compute_node(Project_ID, Zone, instance['name'], network_target, instance['tags']['fingerprint'])
            print(instance['networkInterfaces'][0]['accessConfigs'][0]['natIP'])
            print("Firewall rule added to instance", instance['name'])
    else:
        print("No instances present")