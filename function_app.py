import json
import uuid
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
from azure.data.tables import TableServiceClient
from azure.data.tables import UpdateMode
from azure.core.exceptions import ResourceNotFoundError
import logging

app = func.FunctionApp()

@app.queue_trigger(arg_name="azqueue", queue_name="newqueue",
                               connection="az104_storage") 
def requeue_trigger(azqueue: func.QueueMessage):
    table_name = ("QueueFuncLogs" + str(uuid.uuid4())).replace("-","")
    logger = AzureTableStorage(table_name=table_name)
    logger.add_log('Queue Function triggered')
    try:
        data = azqueue.get_json()
        logger.add_log(f"Msg Had been read {data}")
    except Exception as ex:
        logger.add_log(f"Error while reading message: {str(ex)}")
        return
    
# def trial():
    try:
        azure_table = AzureTableStorage()
        query = f'''PartitionKey eq '{data.get("PartitionKey")}' and RowKey eq '{data.get("RowKey")}' '''
        results = azure_table.query_entities(query)
        logger.add_log(f"State Table queried and the result count is {len(results)}")
        entity = results[0]
        entity["MsgRead"] = True
        azure_table.update_entity(entity)
        logger.add_log(f"MsgRead field has been updated")
    except Exception as ex:
        logger.add_log(f"Error while updating entity: {str(ex)}")
        return

    try:
        name = "Shilajit"
        vault_manager = KeyVaultManager("https://rsc-config2.vault.azure.net/")
        data = vault_manager.get_json_secret("rsc-data2")
        if not data:
            logger.add_log(f"Hey {name}, sorry we couldn't load the secret value. Stopping execution!")
            return
    except Exception as ex:
        logger.add_log(f"Error while getting secret from Key Vault: {str(ex)}")
        return

    try:
        for key, val in data.items():
            if not key.lower().endswith("id") and key not in ["location", "client_secret", "username", "password"]:
                data[key] = "temp"+val
                
        base_resource_group_name = "vm_set"

        # Instantiate the class
        provisioner = AzureVMProvisioner(data.get("subscription_id"), data.get("resource_group_name"), data.get("location"), data.get("tenant_id"), data.get("client_id"), data.get("client_secret"))
        logger.add_log(f"VM Provision instantiated")
    except Exception as ex:
        logger.add_log(f"Error while preparing data for vm provisioning: {str(ex)}")
        return

    try:
        if not entity.get("ResourceGroupCreated", False):
            provisioner.create_resource_group()
            entity["ResourceGroupCreated"] = True
            azure_table.update_entity(entity)
            logger.add_log(f"Resource group {base_resource_group_name} created")
    except Exception as ex:
        logger.add_log(f"Error while creating resource group: {str(ex)}")
        return

    try:
        if not entity.get("VnetCreated", False):
            provisioner.create_virtual_network(data.get("vnet_name"), "10.0.0.0/16")
            entity["VnetCreated"] = True
            azure_table.update_entity(entity)
            logger.add_log(f"Virtual network {data.get('vnet_name')} created")
    except Exception as ex:
        logger.add_log(f"Error while creating virtual network: {str(ex)}")
        return

    try:
        if not entity.get("SubnetCreated", False):
            subnet_result = provisioner.create_subnet(data.get("vnet_name"), data.get("subnet_name"), "10.0.0.0/24")
            entity["SubnetCreated"] = subnet_result.id
            azure_table.update_entity(entity)
            logger.add_log(f"Subnet {data.get('subnet_name')} created")
    except Exception as ex:
        logger.add_log(f"Error while creating subnet: {str(ex)}")
        return

    try:
        if not entity.get("IPCreated", False):
            ip_result = provisioner.create_public_ip(data.get("ip_name"))
            entity["IPCreated"] = ip_result.id
            azure_table.update_entity(entity)
            logger.add_log(f"Public IP {data.get('ip_name')} created")
    except Exception as ex:
        logger.add_log(f"Error while creating public IP: {str(ex)}")
        return

    try:
        if not entity.get("NICCreated", False):
            nic_result = provisioner.create_network_interface(data.get("nic_name"), entity["SubnetCreated"], entity["IPCreated"], data.get("ip_config_name"))
            entity["NICCreated"] = nic_result.id
            azure_table.update_entity(entity)
            logger.add_log(f"Network interface {data.get('nic_name')} created")
    except Exception as ex:
        logger.add_log(f"Error while creating network interface: {str(ex)}")
        return

    try:
        if not entity.get("IdentityCreated", False):
            identity = provisioner.create_user_assigned_identity(base_resource_group_name, "DriverHostVMAccess", "eastus")
            entity["IdentityCreated"] = identity.id
            azure_table.update_entity(entity)
            logger.add_log(f"User assigned identity {identity.id} attached to VM")
    except Exception as ex:
        logger.add_log(f"Error while creating user-assigned identity: {str(ex)}")
        return

    try:
        if not entity.get("VMCreated", False):
            vm_result = provisioner.create_virtual_machine(data.get("vm_name"), entity["NICCreated"], data.get("username"), data.get("password"), entity["IdentityCreated"])
            entity["VMCreated"] = vm_result.id
            azure_table.update_entity(entity)
            logger.add_log(f"Virtual machine {data.get('vm_name')} created")
    except Exception as ex:
        logger.add_log(f"Error while creating virtual machine: {str(ex)}")
        return

    try:
        cmds = {
                "DownloadCommand": "/opt/download --account-name az104storagesh --container-name executable --blob-name create-resources",
                "MakeCommandExecutable": "chmod +x /opt/create-resources",
                "ExecuteCommand": "/opt/create-resources"
            }
        for cmd_name, cmd in cmds.items():
            if not entity.get(cmd_name, False):
                provisioner.run_command_on_vm(data.get("vm_name"), cmd)
                entity[cmd_name] = True
                azure_table.update_entity(entity)
                logger.add_log(f"Command {cmd_name} executed on VM")
    except Exception as ex:
        logger.add_log(f"Error while executing commands on VM: {str(ex)}")
        return

    try:
        if data.get("resource_group_name"):
            logger.add_log(f"Hello, {name}. The {data.get('resource_group_name')} created successfully!")
        else:
            vm_name = data.get("vm_name")
            logger.add_log(f"Hey {name}, sorry we couldn't created the VM '{vm_name}' :(",status_code=200)
    except Exception as ex:
        logger.add_log(f"Error while logging final message: {str(ex)}")
        return


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
        logging.info(f"Provisioned resource group {rg_result.name} in the {rg_result.location} region")
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
        logging.info(f"Provisioned virtual network {vnet_result.name} with address prefixes {vnet_result.address_space.address_prefixes}")
        return vnet_result

    def create_subnet(self, vnet_name, subnet_name, address_prefix):
        poller = self._network_client.subnets.begin_create_or_update(
            self._resource_group_name,
            vnet_name,
            subnet_name,
            {"address_prefix": address_prefix},
        )
        subnet_result = poller.result()
        logging.info(f"Provisioned virtual subnet {subnet_result.name} with address prefix {subnet_result.address_prefix}")
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
        logging.info(f"Provisioned public IP address {ip_address_result.name} with address {ip_address_result.ip_address}")
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
        logging.info(f"Provisioned network interface {nic_result.name}")
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
        logging.info(f"Provisioned virtual machine {vm_result.name}")
         
        return vm_result
    
    def create_user_assigned_identity(self, resource_group_name, identity_name, location):
        """Create or retrieve a User-Assigned Managed Identity."""
        identity = self._msi_client.user_assigned_identities.create_or_update(
            resource_group_name,
            identity_name,
            {"location": location}    # specify the location of the managed identity resource
        )
        logging.info(f"Created or retrieved user-assigned identity: {identity_name}")
        return identity
        
    def run_command_on_vm(self, vm_name: str, cmd: str):
        # Copy script to VM using Run Command
        logging.info(f"Running script on VM {vm_name}...")
        command_parameters = RunCommandInput(
            command_id="RunShellScript",
            script=[
                cmd
            ],
        )
        poller = self._compute_client.virtual_machines.begin_run_command(
            self._resource_group_name,
            vm_name,
            command_parameters
        )
        result = poller.result()
        logging.info(f"Script executed on VM {vm_name}. Result: {result}")

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


class AzureTableStorage:
    def __init__(self, account_name="az104storagesh", table_name="DRStateTable"):
        """
        Initializes the AzureTableStorage instance.
        :param account_name: Azure Storage account name.
        :param table_name: Name of the table to interact with.
        """
        self.account_name = account_name
        self.table_name = table_name
        self.table_service_client = self._authenticate_with_default_credential()
        self.table_client = self._get_or_create_table()

    def _authenticate_with_default_credential(self):
        """
        Authenticates to Azure Table Storage using DefaultAzureCredential.
        :return: TableServiceClient instance.
        """
        try:
            credential = DefaultAzureCredential()
            endpoint = f"https://{self.account_name}.table.core.windows.net"
            return TableServiceClient(endpoint=endpoint, credential=credential)
        except Exception as e:
            logging.info(f"Authentication failed: {e}")
            raise

    def _get_or_create_table(self):
        """
        Gets the table client or creates the table if it does not exist.
        :return: TableClient instance.
        """
        
        return self.table_service_client.create_table_if_not_exists(table_name=self.table_name)
        

    def insert_entity(self, entity):
        """
        Inserts a new entity into the table.
        :param entity: A dictionary representing the entity (must include PartitionKey and RowKey).
        :return: Response from the Azure Table API.
        """
        try:
            return self.table_client.create_entity(entity=entity)
        except Exception as e:
            logging.info(f"Error inserting entity: {e}")
            raise

    def get_entity(self, partition_key, row_key):
        """
        Retrieves an entity by PartitionKey and RowKey.
        :param partition_key: The PartitionKey of the entity.
        :param row_key: The RowKey of the entity.
        :return: The entity if found.
        """
        try:
            return self.table_client.get_entity(partition_key=partition_key, row_key=row_key)
        except ResourceNotFoundError:
            logging.info(f"Entity with PartitionKey={partition_key} and RowKey={row_key} not found.")
            return None

    def update_entity(self, entity):
        """
        Updates an existing entity in the table.
        :param entity: A dictionary representing the entity (must include PartitionKey and RowKey).
        :return: Response from the Azure Table API.
        """
        try:
            return self.table_client.update_entity(entity=entity, mode=UpdateMode.MERGE)
        except Exception as e:
            logging.info(f"Error updating entity: {e}")
            raise

    def delete_entity(self, partition_key, row_key):
        """
        Deletes an entity from the table.
        :param partition_key: The PartitionKey of the entity.
        :param row_key: The RowKey of the entity.
        :return: Response from the Azure Table API.
        """
        try:
            self.table_client.delete_entity(partition_key=partition_key, row_key=row_key)
            logging.info(f"Entity with PartitionKey={partition_key} and RowKey={row_key} deleted successfully.")
        except ResourceNotFoundError:
            logging.info(f"Entity with PartitionKey={partition_key} and RowKey={row_key} not found.")
        except Exception as e:
            logging.info(f"Error deleting entity: {e}")
            raise

    def add_log(self, log_message):
        """
        Adds a log message to the table.
        :param log_message: The log message to be added.
        :return: Response from the Azure Table API.
        """
        try:
            log_entity = {
                'PartitionKey': 'Logs',
                'RowKey': str(uuid.uuid4()),
                'Message': log_message
            }
            logging.info(log_message)
            return self.insert_entity(log_entity)
        except Exception as e:
            print(f"Error adding log: {e}")
            raise

    def query_entities(self, filter_query):
        """
        Queries entities in the table using OData filter syntax.
        :param filter_query: OData filter query string.
        :return: List of matching entities.
        """
        try:
            return list(self.table_client.query_entities(query_filter=filter_query))
        except Exception as e:
            logging.info(f"Error querying entities: {e}")
            raise

# trial()