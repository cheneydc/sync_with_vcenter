# encoding: utf-8
#!/usr/bin/python

"""
Sync the VMs on Vcenter to openstack
"""

from pyVim import connect
from pyVmomi import vmodl
from pyVmomi import vim
import time

from novaclient import client

from insert_vcenter_vm import writeData
from insert_vcenter_vm import connectSQL
from insert_vcenter_vm import closeSQL


# IP and password for vcenter server
HOST        =   "192.168.250.34"
PASSWORD    =   "123qweP" 
USER        =   "dc"

# Address and password for openstack database
DBHOST      =   "10.2.2.10"
DBUSER      =   "root"
DBPASSWD    =   "123qweP"
DATABASE    =   "nova"

# Nova client
PROJECT     =   "admin"
NOVA_USER   =   "admin"
VERSION     =   2
AUTHURI     =   "http://10.2.2.10:5000/v2.0"

# Power state
NOSTATE     =   0x00
RUNNING     =   0x01
PAUSED      =   0x03
SHUTDOWN    =   0x04  # the VM is powered off
CRASHED     =   0x06
SUSPENDED   =   0x07

global content

#VM state
#ACTIVE = 'active'  # VM is running
#BUILDING = 'building'  # VM only exists in DB
#PAUSED = 'paused'
#SUSPENDED = 'suspended'  # VM is suspended to disk.
#STOPPED = 'stopped'  # VM is powered off, the disk image is still there.
#RESCUED = 'rescued'  # A rescue image is running with the original VM image
# attached.
#RESIZED = 'resized'  # a VM with the new size is active. The user is expected
# to manually confirm or revert.
#SOFT_DELETED = 'soft-delete'  # VM is marked as deleted but the disk images are
# still available to restore.
#DELETED = 'deleted'  # VM is permanently deleted.
#ERROR = 'error'
#SHELVED = 'shelved'  # VM is powered off, resources still on hypervisor
#SHELVED_OFFLOADED = 'shelved_offloaded'  # VM and associated resources are
# not on hypervisor


def addFlavor(name, ram, vcpus, disk):
    nova = client.Client(VERSION, NOVA_USER, DBPASSWD, PROJECT, AUTHURI) 
    flavor = nova.flavors
    flavor.create(name=name, ram=ram, vcpus=vcpus, disk=disk)

    return flavor.find(name=name).id


def getVMDetails(vm, depth=10):
    """
    Get the specific data of vm
    """
    global content
    maxdepth = 10
    if hasattr(vm, 'vAppConfig'):
        # TODO  if use vApp need other operation?
        return None 
    if hasattr(vm, 'childEntity'):
        return None

    curTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    vmDetails = {
                    'created_at'     :   curTime,
                    #'update_at'     :   curTime,
                    'launched_at'    :   curTime,
                    #'delete_at'     :   'NULL',
                    #'internal_id'   :   'NULL',

                    # TODO  can get user id from keystone api 
                    # $keystone user-list
                    'user_id'       :   '9855e58a26d84f80a135053fc6b3270f',
                    'launched_on'   :   'B-N-ops-compute-2-10',

                    # TODO  how get project id ?
                    'project_id'    :   '6f70d3bd1e504e2f8ac7a0e6fda0d28e',
                    'availability_zone':'nova',
                    'host'          :   'B-N-ops-compute-2-10',
                    'reservation_id':   '',
                    # TODO get the cluster
                    'node'          :   '',
                    
                    # TODO how get image_ref
                    'image_ref'     :   '',
                    'hostname'      :   'unknown',
                    'deleted'       :   0,
                    'cleaned'       :   0,
                    'display_description':'',
                    'vm_state'      :   'active',
                    'root_gb'       :   0,
                    'ephemeral_gb'  :   0,
                }

    def datastoreVMList(datastore):
        """
        Get vm name list form datastore
        """
        vmNameList = []
        if datastore is not None:
            for vm in datastore.vm:
                if vm is not None:
                    vmNameList.append(vm.name)

        return vmNameList

    def clusterVMList():
        """
        Get vm name list form cluster
        """
        obj_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ClusterComputeResource], True)
        if obj_view is not None:
            clusterList = {} 
            for cluster in obj_view.view:
                clusterName = str(cluster).split(":")[1][:-1] + "(" + cluster.name + ")"
                vmNameList = []
                for datastore in cluster.datastore:
                    vmNameList.extend(datastoreVMList(datastore))
                clusterList[clusterName] = vmNameList 
        
        return clusterList

    summary = vm.summary
    tools_version = summary.guest.toolsStatus

    if tools_version != "toolsNotInstalled":
        guest = vm.guest
    else:
        guest = None

    # TODO If the guest is already managed by openstack
    # then skip it
    if summary.config.managedBy is not None:
        if summary.config.managedBy.extensionKey == "org.openstack.compute":
            return None

    # Get Name
    vmDetails['display_name'] = summary.config.name
    if vmDetails['display_name'] is not None:
        clusterVMList = clusterVMList()
        for node in clusterVMList.keys():
            if vmDetails['display_name'] in clusterVMList[node]:
                vmDetails['node'] = node 

    # Get vm path
    # vmDetails['path'] = summary.config.vmPathName

    # Get Instance uuid
    vmDetails['uuid'] = summary.config.uuid

    # Get vcpus
    vmDetails['vcpus'] = summary.config.numCpu

    # Get memory size
    vmDetails['memory_mb'] = summary.config.memorySizeMB

    # Description
    if summary.config.annotation is not None:
        vmDetails['display_description'] = summary.config.annotation
    else:
        vmDetails['display_description'] = summary.config.name

    # get vm power state
    # poweredOff    The virtual machine is currently powered off. 
    # poweredOn   The virtual machine is currently powered on. 
    # suspended   The virtual machine is currently suspended. 
    vmPowerState = {"poweredOff": SHUTDOWN,
                    "poweredOn" : RUNNING,
                    "suspended" : SUSPENDED}
    vmDetails['power_state'] = vmPowerState[summary.runtime.powerState]

    if guest is not None:
        # Get disk size of vm
        root_ca = 0
        diskInfo = guest.disk
        for partiton in diskInfo:
            root_ca = partiton.capacity + root_ca
        vmDetails['root_gb'] = root_ca/1000/1000/1000

        # Get host name
        vmDetails['hostname'] = guest.hostName 

        # Get vm state
        vmState = {"running": "active",
                   "shuttingdown": "active",
                   "resetting": "active", # ?
                   "standby": "active", # ?
                   "unknown": "active", # ?
                   "notrunning": "stopped"}
        vmDetails["vm_state"] =vmState[guest.guestState] 

    return vmDetails


def getVMExtra(vmDetails):
    """
    Get the data to insert to instance_extra talbe
    """
    if vmDetails['root_gb'] > 0:
        flavorId = addFlavor("VC."+vmDetails['display_name'],
                  vmDetails['memory_mb'],
                  vmDetails['vcpus'],
                  vmDetails['root_gb'])
    else:
        flavorId = '1'

    flavorData = '''{\"new\": null,\"old\": null,\"cur\": {\"nova_object.version\": \"1.1\",\"nova_object.name\": \"Flavor\",\"nova_object.data\": {\"flavorid\": \"%s\"},\"nova_object.namespace\": \"nova\"}}'''
    vmExtra = {
                'flavor'        :  flavorData % str(flavorId), 
                'deleted'       :   0,
                'pci_requests'  :   "[]",
            }

    vmExtra['created_at'] = vmDetails['created_at']
    vmExtra['instance_uuid'] = vmDetails['uuid']

    return vmExtra


def getVMIdMapping(vmDetails):
    """
    Get the data to insert to instance_id_mapping table
    """
    vmIdMapping = {
                'deleted'       :   0,
            }

    vmIdMapping['created_at'] = vmDetails['created_at']
    vmIdMapping['uuid'] = vmDetails['uuid']

    return vmIdMapping


# TODO def getFixedIP(vm):


def printData(vmData):
    """
    Output data
    """
    print "-------------------------------------------"
    if vmData is not None:
        for i in vmData.keys():
            print "%s\t:\t%s" %(i,str(vmData[i]))
    print "-------------------------------------------"
    print ""


def syncWithVcenter(host=HOST, user=USER, pwd=PASSWORD,
                    dbhost=DBHOST, dbuser=DBUSER, dbpwd=DBPASSWD, 
                    database=DATABASE):
    global content
    try:
        dbItem = connectSQL(dbhost, dbuser, dbpwd, database)

        service_instance = connect.SmartConnect(host=host, user=user, pwd=pwd)

        content = service_instance.RetrieveContent()
        children = content.rootFolder.childEntity

        vmDetailsList = []  # insert to table instances
        vmDetailsData = {
                "tableName": "instances",
                }

        vmExtraList = []        # insert to table instances_extra
        vmExtraData = {
                "tableName": "instance_extra",
                }

        vmIdMappingList = []    # insert to talbe instances_id_mapping
        vmIdMappingData = {
                "tableName": "instance_id_mappings",
                }

        for child in children:
            if hasattr(child, 'vmFolder'):
                datacenter = child
            else:
                continue

            vmFolder = datacenter.vmFolder
            vmList = vmFolder.childEntity
            for vm in vmList:
                vmDetails = getVMDetails(vm) 
                if vmDetails is not None:
                    vmExtra = getVMExtra(vmDetails)
                    vmIdMapping = getVMIdMapping(vmDetails)
                    vmDetailsList.append(vmDetails)
                    vmExtraList.append(vmExtra)
                    vmIdMappingList.append(vmIdMapping)

            vmDetailsData["data"] = vmDetailsList
            vmExtraData["data"] = vmExtraList
            vmIdMappingData["data"] = vmIdMappingList

            writeData(dbItem[0], dbItem[1], vmDetailsData)
            writeData(dbItem[0], dbItem[1], vmExtraData)
            writeData(dbItem[0], dbItem[1], vmIdMappingData)     
        
        closeSQL(dbItem[0], dbItem[1])

    except vmodl.MethodFault as error:
        print "Caught vmodl fault : " + error.msg
        return -1

    return 0
            

if __name__ == "__main__":
    syncWithVcenter()
