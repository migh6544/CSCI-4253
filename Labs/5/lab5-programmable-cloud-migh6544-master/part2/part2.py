#!/usr/bin/env python3

import argparse
import os
import time
from pprint import pprint

import googleapiclient.discovery
import google.auth

credentials, project = google.auth.default()
service = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)

# Lists all instances
def list_instances(compute, project, zone):
    result = compute.instances().list(project = project, zone = zone).execute()
    return result['items'] if 'items' in result else None

def create_snapshot(compute, project, zone, existing_vm_name, snapshot_name, labelFingerprint):
    snapshot_body = {
        "name": snapshot_name,
        "labelFingerprint": labelFingerprint,
        "storageLocations":
        [
            "us-west1"
        ]
    }
    return compute.disks().createSnapshot(project = project, zone = zone, disk = existing_vm_name, body = snapshot_body).execute()

def create_snapshot_image(compute, project, zone, snapshot_name, labelFingerprint):
    image_snapshot_body = {
        "name": snapshot_name,
        "sourceSnapshot": "global/snapshots/%s"%(snapshot_name),
        "labelFingerprint": labelFingerprint
    }
    return compute.images().insert(project=project, body=image_snapshot_body).execute()

# Create instance
def create_instance(compute, project, zone, name, bucket):
    image_response = compute.images().getFromFamily(
        project = 'ubuntu-os-cloud', family='ubuntu-2204-lts').execute()
    source_disk_image = "global/snapshots/base-snapshot-csci4253-lab5-part1"

    # Configure machine
    machine_type = "zones/%s/machineTypes/f1-micro" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'startup_script.sh'), 'r').read()
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
                    'sourceSnapshot': source_disk_image,
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
def wait_for_operation(compute, project, zone, operation, type = "zone"):
    print('Waiting for operation to finish...')
    while True:
        result = None
        if type == "zone":
            result = compute.zoneOperations().get(
                project = project,
                zone = zone,
                operation = operation).execute()
        else:
            result = compute.globalOperations().get(
                project = project,
                operation = operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result
        time.sleep(1)


def add_firewall_rule_to_compute_node(project, zone, instance, target_tag, fingerprint):
    tags_body = {
        "items": [
            target_tag
        ],
        "fingerprint": fingerprint
    }
    request = service.instances().setTags(project = project, zone=zone, instance=instance, body = tags_body)
    response = request.execute()


# Run
if __name__ == '__main__':
    Project_ID = 'csci-4253-359820'
    Bucket_Name = 'csci4253-lab5-bucket'
    Zone = 'us-west1-b'  
    
    instances = list_instances(service, Project_ID, 'us-west1-b')
    no_of_clones = 3
    network_target="allow-5000"

    if instances:
        for instance in instances:
            print('Creating snapshot of compute VM :', instance['name'])

            snapshot_name = "base-snapshot-"+instance['name']
            operation = create_snapshot(service, Project_ID, Zone, instance['name'], snapshot_name, instance['labelFingerprint'])
            wait_for_operation(service, Project_ID, Zone, operation['name'])

            for i in range(0, no_of_clones):
                start_time = time.time()
                instance_name = "%s-%d" % (instance['name'], i)
                operation = create_instance(service, Project_ID, Zone, instance_name, snapshot_name)
                wait_for_operation(service, Project_ID, Zone, operation['name'])
                print("--- Clone %d took %s seconds ---" % (i, time.time() - start_time))
            result_instances = list_instances(service, Project_ID, Zone)
            
            if result_instances:
                for result_instance in result_instances:
                    add_firewall_rule_to_compute_node(Project_ID, Zone, result_instance['name'], network_target, result_instance['tags']['fingerprint'])
