import json
import azure.functions as func
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import ContainerClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.keyvault.secrets import SecretClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.compute.models import RunCommandInput
from azure.mgmt.msi import ManagedServiceIdentityClient
import logging

app = func.FunctionApp()

@app.queue_trigger(arg_name="azqueue", queue_name="newqueue",
                               connection="az104_storage") 
def requeue_trigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s',
                azqueue.get_body().decode('utf-8'))
    name = "Shilajit"
    storage_account_name = "az104storagesh"
    container_name = "myfirst"
    blob_name = "b.html"
    
    manager = ContainerManager(storage_account_name, container_name, "c.html")
    print(manager.blob_name)
    print(f"The blob {manager.blob_name} exists: {manager.exists()}")
    manager.blob_name = blob_name
    print(f"The blob {manager.blob_name} exists: {manager.exists()}")
    vault_manager = KeyVaultManager("https://rsc-config2.vault.azure.net/")
    data = vault_manager.get_json_secret("rsc-data2")
    if not data:
        print(f"Hey {name}, sorry we couldn't load the secret value",status_code=200)
    print(data.get("vm_name"))
    for key, val in data.items():
        if not key.lower().endswith("id") and key not in ["location", "client_secret", "username", "password"]:
            data[key] = "temp"+val
            
    base_resource_group_name = "Primary"

    # Instantiate the class
    provisioner = AzureVMProvisioner(data.get("subscription_id"), data.get("resource_group_name"), data.get("location"), data.get("tenant_id"), data.get("client_id"), data.get("client_secret"))

    # Create resources
    provisioner.create_resource_group()
    provisioner.create_virtual_network(data.get("vnet_name"), "10.0.0.0/16")
    subnet_result = provisioner.create_subnet(data.get("vnet_name"), data.get("subnet_name"), "10.0.0.0/24")
    ip_result = provisioner.create_public_ip(data.get("ip_name"))
    nic_result = provisioner.create_network_interface(data.get("nic_name"), subnet_result.id, ip_result.id, data.get("ip_config_name"))
    identity = provisioner.create_user_assigned_identity(base_resource_group_name, "Base")
    provisioner.create_virtual_machine(data.get("vm_name"), nic_result.id, data.get("username"), data.get("password"), identity.id)
       
    provisioner.run_command_on_vm(data.get("vm_name"))
    
    if data.get("resource_group_name"):
        print(f"Hello, {name}. The {data.get('resource_group_name')} created successfully!")
    else:
        vm_name = data.get("vm_name")
        print(f"Hey {name}, sorry we couldn't created the VM '{vm_name}' :(",status_code=200)


class ContainerManager:
    def __init__(self, account_name, container_name, blob_name):
        self._account_name = account_name
        self.__container_name = container_name
        self.__blob_name = blob_name
        self.__credential = DefaultAzureCredential()
        
    @property
    def blob_name(self):
        return self.__blob_name

    @blob_name.setter
    def blob_name(self, name):
        self.__blob_name = name
        
    @property
    def container_name(self):
        return self.__container_name

    @container_name.setter
    def container_name(self, name):
        self.__container_name = name
    
    def get_container_client(self):
        self.Containerclient = ContainerClient(account_url=f"https://{self._account_name}.blob.core.windows.net/", container_name=self.container_name, credential=self.__credential)
        return self.Containerclient

    def exists(self):
        self.blob_client = self.get_container_client().get_blob_client(self.blob_name)
        return self.blob_client.exists()

    def get_blob_client(self):
        self.blob_client = self.get_container_client().get_blob_client(self.blob_name)
        return self.blob_client

    def get_blob_content(self):
        blob_client = self.get_blob_client(self.blob_name)
        return blob_client.download_blob()

class AzureVMProvisioner:
    def __init__(self, subscription_id, resource_group_name, location, tenant_id, client_id, client_secret):
        self._subscription_id = subscription_id
        self._resource_group_name = resource_group_name
        self._location = location
        self._credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        
        # Initialize clients
        self._resource_client = ResourceManagementClient(self._credential, self._subscription_id)
        self._network_client = NetworkManagementClient(self._credential, self._subscription_id)
        self._compute_client = ComputeManagementClient(self._credential, self._subscription_id)
        self._auth_client = AuthorizationManagementClient(self._credential, self._subscription_id)
        self._msi_client = ManagedServiceIdentityClient(self._credential, self._subscription_id)

    def create_resource_group(self):
        rg_result = self._resource_client.resource_groups.create_or_update(
            self._resource_group_name, {"location": self._location}
        )
        print(f"Provisioned resource group {rg_result.name} in the {rg_result.location} region")
        return rg_result

    def create_virtual_network(self, vnet_name, address_prefix):
        poller = self._network_client.virtual_networks.begin_create_or_update(
            self._resource_group_name,
            vnet_name,
            {
                "location": self._location,
                "address_space": {"address_prefixes": [address_prefix]},
            },
        )
        vnet_result = poller.result()
        print(f"Provisioned virtual network {vnet_result.name} with address prefixes {vnet_result.address_space.address_prefixes}")
        return vnet_result

    def create_subnet(self, vnet_name, subnet_name, address_prefix):
        poller = self._network_client.subnets.begin_create_or_update(
            self._resource_group_name,
            vnet_name,
            subnet_name,
            {"address_prefix": address_prefix},
        )
        subnet_result = poller.result()
        print(f"Provisioned virtual subnet {subnet_result.name} with address prefix {subnet_result.address_prefix}")
        return subnet_result

    def create_public_ip(self, ip_name):
        poller = self._network_client.public_ip_addresses.begin_create_or_update(
            self._resource_group_name,
            ip_name,
            {
                "location": self._location,
                "sku": {"name": "Standard"},
                "public_ip_allocation_method": "Static",
                "public_ip_address_version": "IPV4",
            },
        )
        ip_address_result = poller.result()
        print(f"Provisioned public IP address {ip_address_result.name} with address {ip_address_result.ip_address}")
        return ip_address_result

    def create_network_interface(self, nic_name, subnet_id, ip_id, ip_config_name):
        poller = self._network_client.network_interfaces.begin_create_or_update(
            self._resource_group_name,
            nic_name,
            {
                "location": self._location,
                "ip_configurations": [
                    {
                        "name": ip_config_name,
                        "subnet": {"id": subnet_id},
                        "public_ip_address": {"id": ip_id},
                    }
                ],
            },
        )
        nic_result = poller.result()
        print(f"Provisioned network interface {nic_result.name}")
        return nic_result

    def create_virtual_machine(self, vm_name, nic_id, username, password, identity_id):
           
        poller = self._compute_client.virtual_machines.begin_create_or_update(
            self._resource_group_name,
            vm_name,
            {
                "location": self._location,
                "storage_profile": {
                    'image_reference': {
                        'id' : '/subscriptions/02f031f1-f05f-4709-8cf7-68d2e343065d/resourceGroups/vm_set/providers/Microsoft.Compute/galleries/Source/images/Base/versions/1.0.1'
                    }
                },
                "hardware_profile": {"vm_size": "Standard_B1s"},
                "os_profile": {
                    "computer_name": vm_name,
                    "admin_username": username,
                    "admin_password": password,
                },
                "network_profile": {
                    "network_interfaces": [
                        {
                            "id": nic_id,
                        }
                    ]
                },
                "identity": {
                    "type": "UserAssigned",  # Specifies that we are using a user-assigned identity
                    "user_assigned_identities": {
                    identity_id: {}  # Correct format: resource ID as the key, empty dict as the value
                    }
                }
            },
        )
        vm_result = poller.result()
        print(f"Provisioned virtual machine {vm_result.name}")
         
        return vm_result
    
    def create_user_assigned_identity(self, resource_group_name, identity_name):
        """Create or retrieve a User-Assigned Managed Identity."""
        identity = self._msi_client.user_assigned_identities.create_or_update(
            resource_group_name,
            identity_name,
            {"location": self._location}    # specify the location of the managed identity resource
        )
        print(f"Created or retrieved user-assigned identity: {identity_name}")
        return identity
        
    def run_command_on_vm(self, vm_name):
        # Copy script to VM using Run Command
        logging.info(f"Running script on VM {vm_name}...")
        command_parameters = RunCommandInput(
            command_id="RunShellScript",
            script=[
                "/opt/download --account-name az104storagesh --container-name executable --blob-name create-vm",
                "chmod +x create-vm",
                "/opt/create-vm"
            ],
        )
        poller = self._compute_client.virtual_machines.begin_run_command(
            self._resource_group_name,
            vm_name,
            command_parameters
        )
        result = poller.result()
        print(f"Script executed on VM {vm_name}. Result: {result}")

class KeyVaultManager:
    def __init__(self, vault_url: str):
        self._vault_url = vault_url
        self._credentials = DefaultAzureCredential()
        
    
    def get_secret_client(self) -> SecretClient:
        """Get Key Vault secret client"""
        self._secret_client = SecretClient(
            vault_url=self._vault_url,
            credential=self._credentials
        )
        return self._secret_client
    
    def get_secret_value(self, secret_name: str) -> str:
        """Get raw secret value from Key Vault"""
        return self.get_secret_client().get_secret(secret_name).value
    
    def get_json_secret(self, secret_name: str) -> dict:
        """Get and parse JSON secret from Key Vault"""
        secret_value = self.get_secret_value(secret_name)
        return json.loads(secret_value)
