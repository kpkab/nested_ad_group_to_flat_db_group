import requests
from msal import ConfidentialClientApplication
import ast
import json
import configparser
from databricks.sdk import AccountClient
import logging
import os
import datetime
from databricks.sdk.service.iam import ComplexValue
# import time


# Configure logging to both stdout and a log file
log_dir = 'logs'
log_filename = f"{log_dir}/ad_sync_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to stdout
        logging.FileHandler(log_filename)  # Log to a file with timestamp suffix
    ]
)

# Create ConfigParser object and read values from cred.ini file.
config = configparser.ConfigParser()
config.read('cred.ini')

client_id = config.get("azure", "client_id")
client_secret = config.get("azure", "client_secret")
tenant_id = config.get("azure", "tenant_id")
scim_token = config.get("databricks", "scim_token")
scim_url = config.get("databricks", "scim_url")
databricks_account_number = config.get("databricks", "databricks_account_number")
azure_databricks_host = config.get("databricks", "azure_databricks_host")

msal_scope = ["https://graph.microsoft.com/.default"]
msal_authority = f"https://login.microsoftonline.com/{tenant_id}"
a = AccountClient(host=azure_databricks_host, account_id=databricks_account_number)

msal_app = ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=msal_authority
)

# Parse the configuration file and get Azure client and tenant details.
config = configparser.ConfigParser()
config.read('cred.ini')

client_id = config.get('azure', 'client_id')
client_secret = config.get('azure', 'client_secret')
tenant_id = config.get('azure', 'tenant_id')

# define Azure MSAL
msal_authority = f"https://login.microsoftonline.com/{tenant_id}"
msal_scope = ["https://graph.microsoft.com/.default"]
msal_app = ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=msal_authority
)


# acquire Active Directory access Token
def get_access_token():
    """
    Acquires an Azure Active Directory Access Token.

    This function retrieves an access token from Azure Active Directory using Microsoft Authentication Library (MSAL).

    Returns:
        str: Azure Active Directory Access Token.

    Raises:
        Exception: If unable to obtain the access token.
    """
    result = msal_app.acquire_token_silent(
        scopes=msal_scope,
        account=None
    )
    if not result:
        result = msal_app.acquire_token_for_client(
            scopes=msal_scope
        )

    if "access_token" in result:
        access_token = result["access_token"]
    else:
        raise Exception("Couldn't get access token, please check.")
    return access_token


class AzureAPIError(Exception):
    pass


def get_transitive_members_for_group(group_id):
    """
        Retrieve transitive members for a specified Azure Active Directory group.

        This function makes a request to the Microsoft Graph API to retrieve the transitive members
        of a specific Azure Active Directory group identified by its 'group_id'.

        Args:
            group_id (str): The unique identifier of the Azure Active Directory group.

        Returns:
            dict: A dictionary containing the transitive members of the specified group.

        Raises:
            AzureAPIError: If an error occurs during the API request or if the response status code
                           is not 200 (indicating an unsuccessful API call).
    """
    try:
        headers = {
            "Authorization": f"Bearer {get_access_token()}",
            "content-type": "application/json"
        }

        response = requests.get(
            url=f"https://graph.microsoft.com/v1.0/groups/{group_id}/transitiveMembers",
            headers=headers
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise AzureAPIError(f"Error: {response.status_code} - {response.text}")

    except Exception as e:
        raise AzureAPIError(f"An error occurred: {str(e)}")


def get_all_group_details(groups_users, orig_group_details_append, tmp_group_file_name):
    """
        Extracts and stores specific details of Microsoft Graph groups.

        This function processes a list of group-related data ('groups_users') obtained from Microsoft Graph API.
        It filters and extracts specific details ('displayName') of each group and appends them to a list
        ('groups_dict_final'). The function also appends original group details ('orig_group_details_append')
        to the final list and writes the entire group details to a specified file ('tmp_group_file_name').

        Args:
            groups_users (list): List of dictionaries containing group-related data obtained from Microsoft Graph API.
            orig_group_details_append (dict): Original group details to be appended to the final list.
            tmp_group_file_name (str): File name to which the group details will be written.

        Returns:
            list: A list containing dictionaries with the extracted group details.

        Raises:
            Exception: If an error occurs during the processing or writing of group details to the file.
    """
    try:
        all_groups = [d for d in groups_users if d['@odata.type'] == '#microsoft.graph.group']

        keys_to_retain_for_group = ["displayName"]

        # Iterate through each dictionary in the list
        groups_dict_final = []
        groups_dict_final.append((orig_group_details_append))

        for user in all_groups:
            required_group_details = {key: user[key] for key in keys_to_retain_for_group if key in user}
            groups_dict_final.append(required_group_details)

        # Write User details to user file.
        with open(tmp_group_file_name, "a") as tmp_groups_file:
            tmp_groups_file.write(str(groups_dict_final))

        # Append the original
        return groups_dict_final

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


def get_all_user_details(groups_users, tmp_user_file_name):
    """
        Extracts and stores specific details of Microsoft Graph users.

        This function processes a list of user-related data ('groups_users') obtained from Microsoft Graph API.
        It filters and extracts specific details ('userPrincipalName', 'givenName', 'familyName', 'displayName')
        of each user and writes them to a file ('tmp_user_file_name').

        Args:
            groups_users (list): List of dictionaries containing user-related data obtained from Microsoft Graph API.
            tmp_user_file_name (str): File name to which the user details will be written.

        Returns:
            None: Returns None after processing and writing user details to the file.

        Raises:
            Exception: If an error occurs during the processing or writing of user details to the file.
    """
    try:
        all_users = [d for d in groups_users if d['@odata.type'] == '#microsoft.graph.user']

        keys_to_retain_for_user = ["userPrincipalName", "givenName", "familyName", "displayName"]

        # Iterate through each dictionary in the list
        for user in all_users:
            required_user_details = {key: user[key] for key in keys_to_retain_for_user if key in user}

            # Write User details to user file.
            with open(tmp_user_file_name, "a") as tmp_user_file:
                tmp_user_file.write(str(required_user_details) + "\n")
            tmp_user_file.close()

        return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


def get_service_principal(top_level_group_name):
    """
        Retrieves service principal details for a specified top-level group from Microsoft Graph API.

        This function makes a request to the Microsoft Graph API to obtain service principal details
        for a specific top-level group identified by its 'top_level_group_name'.

        Args:
            top_level_group_name (str): The display name of the top-level group to retrieve service principals.

        Returns:
            dict: A dictionary containing Azure service principal details related to the specified top-level group.

        Raises:
            Exception: If an error occurs during the API request or processing the service principal details.
    """
    # define header
    headers = {"Authorization": f"Bearer {get_access_token()}", "content-type": "application/json"}

    # Call MS Graph API to get the group members. At this stage, we are only calling the top level group and its members
    response = requests.get(
        url=f"https://graph.microsoft.com/v1.0/groups?$filter=displayName%20eq%20'{top_level_group_name}'",
        headers=headers
    )

    azure_sp_details = response.json()

    return azure_sp_details


def get_azure_user(user_name, token):
    """
        Retrieves service principal details for a specified top-level group from Microsoft Graph API.

        This function makes a request to the Microsoft Graph API to obtain service principal details
        for a specific top-level group identified by its 'top_level_group_name'.

        Args:
            top_level_group_name (str): The display name of the top-level group to retrieve service principals.

        Returns:
            dict: A dictionary containing Azure service principal details related to the specified top-level group.

        Raises:
            Exception: If an error occurs during the API request or processing the service principal details.
    """
    # define header
    headers = {"Authorization": f"Bearer {token}", "content-type": "application/json"}

    # Call MS Graph API to get the group members. At this stage, we are only calling the top level group and its members
    response = requests.get(
        url=f"https://graph.microsoft.com/v1.0/users?$filter=displayName%20eq%20'{user_name}'",
        headers=headers
    )

    azure_ad_user_details = response.json()

    return azure_ad_user_details



def get_original_group_details(orig_group_id, tokens):
    """
        Retrieves details of the original group from Microsoft Graph API.

        This function makes a request to the Microsoft Graph API to obtain details of the original group
        identified by its 'orig_group_id'.

        Args:
            orig_group_id (str): The unique identifier of the original group.
            tokens (str): Access token for authorization to make the API request.

        Returns:
            dict: A dictionary containing specific details ('displayName') of the original group.

        Raises:
            AzureAPIError: If an error occurs during the API request or if the response status code
                           is not 200 (indicating an unsuccessful API call).
    """
    try:
        headers = {"Authorization": f"Bearer {tokens}", "content-type": "application/json"}

        # Call MS Graph API to get the group details. This is needed because sometimes the top-level group
        # may have some SP that needs to be added to the groups file.
        response = requests.get(
            url=f"https://graph.microsoft.com/v1.0/groups/{orig_group_id}",
            headers=headers
        )
        if response.status_code == 200:
            orig_group_details = response.json()
            return {"displayName": orig_group_details["displayName"]}
        else:
            raise AzureAPIError(f"Error: {response.status_code} - {response.text}")

    except Exception as e:
        raise AzureAPIError(f"An error occurred: {str(e)}")


def get_service_principal_details(groups_file_name, token, sp_file_name):
    """
        Retrieves and logs details of Service Principals associated with groups from Microsoft Graph API.

        This function reads group details from a file ('groups_file_name'), iterates through each group,
        retrieves the associated Service Principals using Microsoft Graph API, and logs their details. It then
        writes specific Service Principal details to a file ('sp_file_name').

        Args:
            groups_file_name (str): The name of the file containing group details.
            token (str): Access token for authorization to make the API request.
            sp_file_name (str): File name to which the Service Principal details will be written.

        Returns:
            str: Message indicating the successful completion of the function.

        Raises:
            Exception: If an error occurs during the processing, API requests, or file writing.
    """
    try:
        with open(groups_file_name, "r") as temp_group_file:
            lines = ast.literal_eval(temp_group_file.read())
            for group in lines:
                logging.info("Now working in group: " + str(group.get("displayName")))
                sp_in_group = get_service_principal(str(group.get("displayName")))
                group_id1 = sp_in_group["value"][0]["id"]
                headers = {"Authorization": f"Bearer {token}", "content-type": "application/json"}
                sp = requests.get(
                    url=f"https://graph.microsoft.com/v1.0/groups/{group_id1}?$expand=members",
                    headers=headers
                )
                group_members = sp.json()["members"]
                logging.info("Now will look if this group has any Service Principals.")
                if len(group_members) > 0:
                    # if the group has members, then look for service principals.
                    for sps in group_members:
                        if "#microsoft.graph.servicePrincipal" in sps.values():
                            logging.info(f"The EntraID Group {group['displayName']} has the following "
                                         f"Service Principals.")
                            logging.info("Service Principal Name: " + sps["displayName"])
                            required_for_sp = {
                                "account_id": databricks_account_number,
                                "id": sps["id"],
                                "displayName": sps["displayName"],
                                "applicationId": sps["id"],
                                "active": "true"
                            }
                            with open(sp_file_name, "a") as sp_file:
                                sp_file.write(str(required_for_sp) + "\n")

                        else:
                            logging.info(f"The EntraID Entity {sps['displayName']} is not a Service principals.")
                else:
                    logging.info(f"The EntraID Group {group['displayName']} does not have any members.")
            return "get_service_principal_details Function completed successfully."

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


def create_databricks_group(group_name):
    """
        Creates a group in Databricks with a name matching the group in Azure Active Directory.

        This function attempts to create a group in Databricks with the same display name as the group in Azure Active Directory.

        Args:
            group_name (str): The display name of the group to be created in Databricks.

        Returns:
            bool: True if the Databricks group creation is successful, False otherwise.

        Raises:
            Exception: If an error occurs during the Databricks group creation process.
    """
    # This function will create a group in Databricks with the same name in Azure AD.
    try:
        databricks_group_creation = a.groups.create(display_name=group_name)
        print(databricks_group_creation)
        return True
    except Exception as e:
        print("Error: " + str(e))
        return False


def check_db_group_existence(indv_group_id):
    """
        Checks the existence of a group in Databricks using its unique identifier.

        This function verifies the existence of a group in Databricks by querying the group using its unique identifier.
        It logs information regarding the group existence or absence in Databricks.

        Args:
            indv_group_id (str): The unique identifier of the group in Databricks.

        Returns:
            bool: True if the group exists in Databricks, False otherwise.

        Raises:
            None
    """
    try:
        db_group_existence = a.groups.get(id=indv_group_id)
        logging.info(db_group_existence)
        return True
    except Exception as e:
        logging.warning(f"The group {indv_group_id} does not exist in Databricks.")
        logging.warning("Error message from Databricks.")
        logging.warning(f"{e}")
        return False


def filter_tmp_files_by_group_id(directory, group_id):
    """
        Filters files in a directory based on a specified group ID prefix and suffix conditions.

        This function scans a specified directory for files and filters them based on the given 'group_id'.
        It selects files with names starting with the provided 'group_id' and ending with either '_tmp_sp.txt'
        or '_tmp_users.txt'. The filtered filenames are added to a list and returned.

        Args:
            directory (str): The path of the directory containing files.
            group_id (str): The group ID used to filter files by prefix.

        Returns:
            list: A list of filenames matching the conditions of starting with 'group_id' and ending with
                  either '_tmp_sp.txt' or '_tmp_users.txt'.

        Raises:
            None
    """
    matching_files = []

    for filename in os.listdir(directory):
        if filename.startswith(group_id) and (filename.endswith("_tmp_sp.txt") or filename.endswith("_tmp_users.txt")):
            matching_files.append(filename)

    return matching_files


def create_db_account_group(db_group_name):
    """
        Creates an account group in the associated environment.

        This function attempts to create an account group using the provided 'db_group_name'.
        If successful, it returns the created group details; otherwise, it handles the scenario
        where the group already exists or encounters an error during group creation.

        Args:
            db_group_name (str): The display name of the account group to be created.

        Returns:
            dict or str: If the group is successfully created, it returns the details of the created group.
                        If the group already exists, it returns "Exists".
                        In case of an error during group creation, it logs a warning and returns "Exists".

        Raises:
            None
    """
    try:
        create_dba_group = a.groups.create(display_name=db_group_name)
        return create_dba_group
        # return "Created"
    except Exception as e:
        logging.warning(f"Error creating account group '{db_group_name}': {e}")
        # Handle or log the specific error encountered during group creation
        return "Exists"


def create_users_add_to_groups(user_file, create_db_grp, group_value_existing):
    """
        Processes user details from a file and adds users to an existing Databricks group.

        This function reads user details from a file and either creates new users in Databricks
        or adds existing users to the specified Databricks group. It checks if users already exist
        in the Databricks account and, based on that, adds users to the provided Databricks group.

        Args:
            user_file (file): A file containing user details to be processed.
            create_db_grp (Group): An object representing the Databricks group to which users will be added.
            group_value_existing (list): A list of existing user/group details to update in Databricks.

        Returns:
            None

        Raises:
            None
    """
    group_info = []

    for line in user_file:
        display_name = ast.literal_eval(line).get("displayName", "None")
        user_name = ast.literal_eval(line).get("displayName", "None")
        check_if_user_exists_in_dba = a.users.list(filter=f"displayName eq '{display_name}'")
        check_if_user_exists_in_dba_list = list(check_if_user_exists_in_dba)

        if len(check_if_user_exists_in_dba_list):
            # user already exists in the Databricks Account. So user will not be created,
            # but will add user to group. To add user we need to id of the user. so get that first.
            logging.info(f"User {display_name} already exists in Databricks Account, so will add this user"
                         f" to the group. Databricks user creation will be ignored.")
            db_user_id = a.users.list(filter=f"displayName eq {display_name}")
            db_user_id_list = [item for item in db_user_id]

            # logging.info(db_user_id_list[0])
            # logging.info(type(db_user_id_list[0]))
            idd = [user.id for user in db_user_id_list]
            # logging.info(idd)
            # logging.info(type(idd))
            required_db_user_id = idd[0]
            # logging.info(required_db_user_id)

            # time.sleep(5)
            group_value_new = ComplexValue(display=display_name, value=required_db_user_id)
            group_value_existing.append(group_value_new)
            # logging.info(group_value_existing)
            group_member_details = [
                {"display": cv.display, "primary": cv.primary, "type": cv.type, "value": cv.value}
                for cv in group_value_existing
            ]
            # # group_info.append(group_info1)
            # line_counter += 1
            group_value1 = [ComplexValue(**info) for info in group_member_details]
            adding_db_user_to_db_group = a.groups.update(id=create_db_grp.id,
                                                         display_name=create_db_grp.display_name,
                                                         members=group_value1)
            logging.info(f"Existing Users {display_name} added to the Databricks Account Group.")
        else:
            logging.info(f"User {display_name} Does NOT exists in Databricks Account. This user will be created "
                         "in Databricks Account and then be added to the group.")
            db_a_user_creation = a.users.create(active=True, display_name=display_name, user_name=user_name)
            group_value_new = ComplexValue(display=display_name, value=db_a_user_creation.id)
            if group_value_existing:
                group_value_existing.append(group_value_new)
                group_member_details = [
                    {"display": cv.display, "primary": cv.primary, "type": cv.type, "value": cv.value}
                    for cv in group_value_existing
                ]

                group_value1 = [ComplexValue(**info) for info in group_member_details]

                # line_counter += 1
                adding_db_user_to_db_group = a.groups.update(id=create_db_grp.id,
                                                             display_name=create_db_grp.display_name,
                                                             members=group_value1)
            else:
                adding_db_user_to_db_group = a.groups.update(id=create_db_grp.id,
                                                             display_name=create_db_grp.display_name,
                                                             members=[group_value_new])


        logging.info(f"useer {display_name} was created and added to group.")


def create_sps_add_to_groups(sps_file, create_db_grp, group_value_existing):
    """
        Processes Service Principal details from a file and adds Service Principals to an existing Databricks group.

        This function reads SP details from a file and either creates new SP in Databricks
        or adds existing SP to the specified Databricks group. It checks if SP already exist
        in the Databricks account and, based on that, adds SP to the provided Databricks group.

        Args:
            sps_file (file): A file containing SP details to be processed.
            create_db_grp (Group): An object representing the Databricks group to which users will be added.
            group_value_existing (list): A list of existing SP/group details to update in Databricks.

        Returns:
            None

        Raises:
            None
    """
    # logging.info(create_db_grp)
    # logging.info(group_value_existing)
    group_info = []

    for line in sps_file:
        display_name = ast.literal_eval(line).get("displayName", "None")
        user_name = ast.literal_eval(line).get("displayName", "None")
        application_id = ast.literal_eval(line).get("applicationId", "None")

        check_if_sps_exists_in_dba = a.service_principals.list(filter=f"displayName eq '{display_name}'")
        check_if_sps_exists_in_dba_list = list(check_if_sps_exists_in_dba)

        # logging.info(check_if_sps_exists_in_dba_list)


        if len(check_if_sps_exists_in_dba_list):
            # SP already exists in the Databricks Account. So SP will not be created,
            # but will add SP to group. To add user we need to id of the SP. so get that first.
            logging.info(f"Service Principal {display_name} already exists in Databricks Account, so will add this SP"
                         f" to the group. Databricks Service Principal creation will be ignored.")
            db_sps_id = a.service_principals.list(filter=f"displayName eq {display_name}")

            # logging.info(db_sps_id)

            db_sps_id_list = [item for item in db_sps_id]

            # logging.info(db_sps_id_list[0])
            # logging.info(type(db_sps_id_list[0]))
            idd = [user.id for user in db_sps_id_list]
            # logging.info(idd)
            # logging.info(type(idd))
            required_db_sps_id = idd[0]
            # logging.info(required_db_sps_id)

            # time.sleep(5)
            group_value_new = ComplexValue(display=display_name, value=required_db_sps_id)

            group_value_existing.append(group_value_new)

            # logging.info(group_value_existing)
            group_member_details = [
                {"display": cv.display, "primary": cv.primary, "type": cv.type, "value": cv.value}
                for cv in group_value_existing
            ]
            group_value1 = [ComplexValue(**info) for info in group_member_details]
            adding_db_user_to_db_group = a.groups.update(id=create_db_grp.id,
                                                         display_name=create_db_grp.display_name,
                                                         members=group_value1)
            logging.info(f"Existing Users {display_name} added to the Databricks Account Group.")
        else:
            logging.info(f"Service Principal {display_name} Does NOT exists in Databricks Account. "
                         f"This Service Principal will be created in Databricks Account and then added to the group.")

            db_a_sps_creation = a.service_principals.create(active=True, display_name=display_name, application_id=application_id)

            # logging.info(db_a_sps_creation)

            group_value_new = ComplexValue(display=display_name, value=db_a_sps_creation.id)
            if group_value_existing:
                group_value_existing.append(group_value_new)
                group_member_details = [
                    {"display": cv.display, "primary": cv.primary, "type": cv.type, "value": cv.value}
                    for cv in group_value_existing
                ]
                group_value1 = [ComplexValue(**info) for info in group_member_details]
                adding_db_user_to_db_group = a.groups.update(id=create_db_grp.id,
                                                             display_name=create_db_grp.display_name,
                                                             members=group_value1)
            else:
                adding_db_user_to_db_group = a.groups.update(id=create_db_grp.id,
                                                             display_name=create_db_grp.display_name,
                                                             members=[group_value_new])

        logging.info(f"SERVICE PRINCIPAL {display_name} was created and added to group.")



def create_db_users_add_to_group(db_user_file_name, db_group_name):
    """
        Creates Databricks account users and adds them to a specified group.

        This function reads user details from a file ('db_user_file_name') and creates users in the Databricks account.
        It then adds these users to the specified Databricks group ('db_group_name').
        If the group already exists, it retrieves the group's members and updates the group membership with new users.

        Args:
            db_user_file_name (str): The file path containing user details to be processed.
            db_group_name (str): The name of the Databricks group to which users will be added.

        Returns:
            None

        Raises:
            None
    """
    # create databricks account users.
    # read the input user file and loop through the file line by line and create each user.
    with open(db_user_file_name, "r") as user_file:
        logging.info("contents of the user file:")

        line_counter = 0
        create_db_grp = create_db_account_group(db_group_name)

        if create_db_grp != "Exists":
            a1 = create_users_add_to_groups(user_file, create_db_grp,[])
            logging.info(a1)

        elif create_db_grp == "Exists":
            logging.warning("Group Already Exists in Databricks Account. So user will be added to this group.")
            # now get the group id and pass it to the below function.
            # for existing groups, we may need to check if there are existing members. if there are existing
            # members then we need to pull them as and re-apply them to the group along with the new members.
            create_db_grp = a.groups.list(filter=f"displayName eq {db_group_name}")
            create_db_grp_list = [item for item in create_db_grp]
            # logging.info(create_db_grp)
            group_members = [m.members for m in create_db_grp_list]
            # logging.info(group_members[0])
            a2 = create_users_add_to_groups(user_file, create_db_grp_list[0], group_members[0])
            logging.info(a2)


def create_db_sps_add_to_group(db_sps_file_name, db_group_name):
    """
        Creates Databricks account users and adds them to a specified group.

        This function reads user details from a file ('db_user_file_name') and creates users in the Databricks account.
        It then adds these users to the specified Databricks group ('db_group_name').
        If the group already exists, it retrieves the group's members and updates the group membership with new users.

        Args:
            db_sps_file_name (str): The file path containing user details to be processed.
            db_group_name (str): The name of the Databricks group to which users will be added.

        Returns:
            None

        Raises:
            None
    """
    # create databricks account service principals.
    # read the input Service Principal file and loop through the file line by line and create each SP in Databricks.
    with open(db_sps_file_name, "r") as sp_file:
        logging.info("contents of the user file:")

        create_db_grp = create_db_account_group(db_group_name)

        if create_db_grp != "Exists":
            a1 = create_sps_add_to_groups(sp_file, create_db_grp,[])
            logging.info(a1)

        elif create_db_grp == "Exists":
            logging.warning("Group Already Exists in Databricks Account. So SP will be added to this group.")
            # now get the group id and pass it to the below function.
            # for existing groups, we may need to check if there are existing members. if there are existing
            # members then we need to pull them as and re-apply them to the group along with the new members.
            create_db_grp = a.groups.list(filter=f"displayName eq {db_group_name}")
            create_db_grp_list = [item for item in create_db_grp]
            # logging.info(create_db_grp)
            group_members = [m.members for m in create_db_grp_list]
            # logging.info(group_members[0])
            # logging.info(type(create_db_grp))
            a2 = create_sps_add_to_groups(sp_file, create_db_grp_list[0], group_members[0])
            logging.info(a2)



def process_files(matching_files, db_group_name):
    """
        Processes files based on their types (user or service principal) and performs corresponding actions.

        This function analyzes the provided list of file names ('matching_files') to identify the presence of files
        related to users ('_tmp_users.txt') and service principals ('_tmp_sp.txt'). Based on the identified files,
        it initiates the creation of users and/or service principals in the Databricks account.

        Args:
            matching_files (list): A list of file names to be processed.
            db_group_name (str): The name of the Databricks group where users/service principals will be added.

        Returns:
            None

        Raises:
            None
    """

    try:
        has_users = any("_tmp_users.txt" in file for file in matching_files)
        has_sp = any("_tmp_sp.txt" in file for file in matching_files)

        if has_users and has_sp:
            logging.info("Now creating both Users and Service Principals.")
            # create_db_users() get only user files
            users_files = [file for file in matching_files if file.endswith("_tmp_users.txt")]
            user_grp_status = create_db_users_add_to_group("groups_users_sps/"+users_files[0], db_group_name)
            logging.info(user_grp_status)
            # create_db_sp()
            sp_files = [file for file in matching_files if file.endswith("_tmp_sp.txt")]
            sps_grp_status = create_db_sps_add_to_group("groups_users_sps/" + sp_files[0], db_group_name)
            logging.info(sps_grp_status)
        elif has_users:
            logging.info("Now creating only users.")
            # create_db_users()
            users_files = [file for file in matching_files if file.endswith("_tmp_users.txt")]
            user_grp_status = create_db_users_add_to_group("groups_users_sps/"+users_files[0], db_group_name)
            logging.info(user_grp_status)
        elif has_sp:
            logging.info("Now creating only sp.")
            # create_db_sp()
            sp_files = [file for file in matching_files if file.endswith("_tmp_sp.txt")]
            sps_grp_status = create_db_sps_add_to_group("groups_users_sps/" + sp_files[0], db_group_name)
            logging.info(sps_grp_status)
        else:
            # Handle scenario when neither file is present
            logging.error("Something other than Users, Service Principals found. Check the groups_users_sps folder "
                          "for the types of files created.")
            exit(99)

    except Exception as e:
        logging.error(f"Error processing files: {e}")


def clean_up_files(directory_path: object) -> object:
    """
        Cleans up temporary files within the specified directory.

        This function deletes temporary files found within the provided directory path. It iterates through the files
        in the directory and removes them one by one. If no temporary files are present, it logs a message indicating
        their absence.

        Args:
            directory_path (str): The path to the directory containing temporary files.

        Returns:
            None

        Raises:
            None
    """
    tmp_files_folder_name = directory_path
    if os.listdir(tmp_files_folder_name):
        logging.info("Now attempting to delete all temp files.")
        for filename in os.listdir(tmp_files_folder_name):
            file_path = os.path.join(tmp_files_folder_name, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)  # Delete the file
                    logging.info(f"Deleted: {file_path}")
            except Exception as e:
                logging.error(f"Error deleting {file_path}: {e}")
    else:
        logging.info("There are no tmp files to delete.")


def get_group_id_from_name(azure_group_name, tokens):
    """
        Retrieves the Azure Active Directory (AAD) group ID based on the group name.

        This function queries the Microsoft Graph API to find a group's ID by its display name. It uses the provided
        authentication tokens to authorize the request. If the group exists, it returns the group ID; otherwise, it
        returns False.

        Args:
            azure_group_name (str): The display name of the Azure Active Directory group.
            tokens (str): The authentication token required for accessing the Microsoft Graph API.

        Returns:
            Union[str, bool]: Returns the group ID as a string if found, or False if the group doesn't exist.

        Raises:
            AzureAPIError: If an error occurs while querying the Microsoft Graph API or processing the response,
                an AzureAPIError is raised to handle exceptional cases.
    """
    try:
        headers = {"Authorization": f"Bearer {tokens}", "content-type": "application/json"}

        # Call MS Graph API to get the group details. This is needed because sometimes the top-level group
        # may have some SP that needs to be added to the groups file.
        response = requests.get(
            url=f"https://graph.microsoft.com/v1.0/groups?$filter=startswith(displayName,'{azure_group_name}')",
            headers=headers
        )
        if response.status_code == 200:
            orig_group_details = response.json()
            # return {"displayName": orig_group_details["displayName"]}
            if len(orig_group_details['value']) > 0:
                group_id = orig_group_details['value'][0]['id']
                # print(group_id)

                return group_id
            else:
                return False
        else:
            raise AzureAPIError(f"Error: {response.status_code} - {response.text}")

    except Exception as e:
        raise AzureAPIError(f"An error occurred: {str(e)}")


if __name__ == "__main__":

    # Clean up all tmp files
    try:
        clean_up_tmp_files: None = clean_up_files('groups_users_sps')
        logging.info("clean_up_files Function Completed Successfully.")
    except Exception as e:
        logging.error(f"Temp file cleanup function failes.")

    ##########################
    # Get Azure access token #
    ##########################
    try:
        token = get_access_token()
        logging.info(f"Access token acquired successfully.")
    except Exception as e:
        logging.error(f"Access Token Error: {e}")


    ######################################
    # Check the groups_to_sync.json File #
    ######################################
    with open('groups_to_sync.json', 'r') as items:
        items_to_sync = json.load(items)
        # Check what are all the items we need to sync for this run.
        keys = [key for key, value in items_to_sync.items() if isinstance(value, list) and len(value) > 0]
        items_found = []
        for key in keys:
            value = items_to_sync[key]
            if f"{key}" == "group_ids":
                logging.info(f"The following {key} along with its values will be sync'd from your Azure EntraID to "
                             f"Azure Databricks Account.")
                logging.info(f"{key}: {value}")

                for group_id in value:
                    logging.info("####################################################################################")
                    logging.info(f"Now working on GROUP ID: {group_id}")
                    logging.info("The following 3 files will be created for this group id")
                    logging.info(f"Groups File name: groups_users_sps/{group_id}_tmp_groups.txt")
                    logging.info(f"User File name: groups_users_sps/{group_id}_tmp_users.txt")
                    logging.info(f"Service Principal File name: groups_users_sps/{group_id}_tmp_sp.txt")
                    logging.info("####################################################################################")

                    ################################################
                    # Get transitive group members based on GroupID#
                    ################################################
                    try:
                        transitive_members = get_transitive_members_for_group(group_id)
                        logging.info("Transitive members identified.")
                        logging.info("Transitive members can be AD groups or Users or Service Principals.")
                        groups_users = transitive_members["value"]
                        logging.info(groups_users)

                        #####################################################
                        # Append the original group name to the groups file #
                        #####################################################
                        orig_group_details = get_original_group_details(group_id, token)
                        logging.info(orig_group_details)

                        #################
                        # Group details #
                        #################
                        try:
                            all_group = get_all_group_details(groups_users, orig_group_details,
                                                              f"groups_users_sps/{group_id}_tmp_groups.txt")
                            logging.info(all_group)
                        except Exception as e:
                            logging.error(f"get_all_group_details Function encountered an error: {e}")
                            break

                        ################
                        # User details #
                        ################
                        try:
                            all_user = get_all_user_details(groups_users, f"groups_users_sps/{group_id}_tmp_users.txt")
                            logging.info(all_user)
                        except Exception as e:
                            logging.error(f"get_all_user_details Function encountered an error: {e}")
                            break

                        ######################
                        # Service Principals #
                        ######################
                        try:
                            service_principals_details = get_service_principal_details(
                                f"groups_users_sps/{group_id}_tmp_groups.txt", token,
                                f"groups_users_sps/{group_id}_tmp_sp.txt")
                            logging.info(service_principals_details)
                        except Exception as e:
                            logging.error(f"get_service_principal_details Function encountered an error: {e}")
                            break

                    except AzureAPIError as e:
                        logging.error(f"Function encountered an error: {e}")
                    except Exception as e:
                        logging.error(f"Unhandled error occurred: {e}")

            elif f"{key}" == "group_names":
                logging.info(f"The following {key} along with its values will be sync'd from your Azure EntraID to "
                             f"Azure Databricks Account.")
                logging.info(f"{key}: {value}")
                # Using the group name, get the Group ID. Once the group ID is obtained, follow the steps above.
                # call the Azure Graph API to get the group ID using the group name.

                for group_name in value:
                    group_id_from_group_name = get_group_id_from_name(group_name, token)
                    if group_id_from_group_name:
                        logging.info(f"{group_id_from_group_name} is the group id for group name {group_name}")
                        for group_id in value:
                            logging.info(
                                "####################################################################################")
                            logging.info(f"Now working on GROUP ID: {group_id_from_group_name}")
                            logging.info("The following 3 files will be created for this group id")
                            logging.info(f"Group File name: groups_users_sps/{group_id_from_group_name}_tmp_groups.txt")
                            logging.info(f"User File name: groups_users_sps/{group_id_from_group_name}_tmp_users.txt")
                            logging.info(f"Service Principal File name: groups_users_sps/{group_id_from_group_name}"
                                         f"_tmp_sp.txt")
                            logging.info(
                                "####################################################################################")

                            ################################################
                            # Get transitive group members based on GroupID#
                            ################################################
                            try:
                                transitive_members = get_transitive_members_for_group(group_id_from_group_name)
                                logging.info("Transitive members identified.")
                                logging.info("Transitive members can be AD groups or Users or Service Principals.")
                                groups_users = transitive_members["value"]
                                logging.info(groups_users)

                                #####################################################
                                # Append the original group name to the groups file #
                                #####################################################
                                orig_group_details = get_original_group_details(group_id_from_group_name, token)
                                logging.info(orig_group_details)

                                #################
                                # Group details #
                                #################
                                try:
                                    all_group = get_all_group_details(groups_users, orig_group_details,
                                                                      f"groups_users_sps/{group_id_from_group_name}"
                                                                      f"_tmp_groups.txt")
                                    logging.info(all_group)
                                except Exception as e:
                                    logging.error(f"get_all_group_details Function encountered an error: {e}")
                                    break

                                ################
                                # User details #
                                ################
                                try:
                                    all_user = get_all_user_details(groups_users,
                                                     f"groups_users_sps/{group_id_from_group_name}_tmp_users.txt")
                                    logging.info(all_user)
                                except Exception as e:
                                    logging.error(f"get_all_user_details Function encountered an error: {e}")
                                    break

                                ######################
                                # Service Principals #
                                ######################
                                try:
                                    service_principals_details = get_service_principal_details(
                                        f"groups_users_sps/{group_id_from_group_name}_tmp_groups.txt", token,
                                        f"groups_users_sps/{group_id_from_group_name}_tmp_sp.txt")
                                    logging.info(service_principals_details)
                                except Exception as e:
                                    logging.error(f"get_service_principal_details Function encountered an error: {e}")
                                    break

                            except AzureAPIError as e:
                                logging.error(f"Function encountered an error: {e}")
                            except Exception as e:
                                logging.error(f"Unhandled error occurred: {e}")
                    else:
                        logging.error(f"{group_name} Was Not Found in Azure. Exiting.")
                        break

            elif f"{key}" == "users":
                logging.info(f"The following Azure AD {key} will be created in Azure Databricks Account.")
                logging.info(f"{key}: {value}")
                # users: ['DB_User4']
                # Check if this user exists in Azure AD. If Yes, then proceed to next step. Else, exit.
                # If user exists in Azure AD then, check if user exists in Databricks. If user exists, then exit.
                # If user does not exist in Databricks Account, then create the user.
                for user in value:
                    logging.info(f"Validating to make sure that the {user} exists in Azure AD.")
                    azure_ad_user_status = get_azure_user(user, token)
                    if len(azure_ad_user_status['value']) > 0:
                        logging.info(f"User {user} is a valid user in Azure AD. Now will check if this user exists "
                                     f"in Databricks Account before creating.")
                        # check if this user exists in Databricks Account.
                        display_name = azure_ad_user_status['value'][0]['displayName']
                        name = azure_ad_user_status['value'][0]['displayName']

                        check_if_user_exists_in_dba = a.users.list(filter=f"displayName eq '{display_name}'")
                        check_if_user_exists_in_dba_list = list(check_if_user_exists_in_dba)

                        if len(check_if_user_exists_in_dba_list) > 0:
                            logging.info(f"User {display_name} already exists in Databricks Account. No action taken.")
                        else:
                            logging.info(f"User {display_name} will now be created in Databricks Account.")
                            create_db_user = a.users.create(active=True, display_name=display_name,
                                                            user_name=display_name)
                            logging.info(create_db_user)

                    else:
                        logging.error(f"User {user} is not a Valid user in Azure AD.")
                        exit(99)

            items_found.append(key)

    ###############################################################################
    # Now that we got all Azure entities, lets create them in Databricks Account. #
    # at this stage all the temp files are created. We can now use the entries in #
    # those temp files and create those identities in Databricks Account.
    # The files can be grouped with the Azure EntraID group id. Look at the file
    # naming convention.
    ###############################################################################

    tmp_file_location = 'groups_users_sps'
    file_names = [f for f in os.listdir(tmp_file_location) if os.path.isfile(os.path.join(tmp_file_location, f))]
    logging.info(file_names)
    patterns_to_remove = ['_tmp_sp.txt', '_tmp_groups.txt', '_tmp_users.txt']
    unique_ids = list({file.split('_')[0] for file in file_names})
    logging.info(unique_ids)

    for indv_group_id in unique_ids:
        db_group_to_be_created = get_original_group_details(indv_group_id, token)
        logging.info(db_group_to_be_created)
        logging.info(db_group_to_be_created['displayName'])
        # logging.info(f"The following group {db_group_to_be_created['displayName']}"
        #              f" will be created in Databricks Account.")


        # Check if this group already exists in Databricks Account.
        check_if_group_present_in_db = check_db_group_existence(indv_group_id)
        if check_if_group_present_in_db:
            logging.info(f"The group: {db_group_to_be_created['displayName']} is present in Databricks already.")
            # if the group is present in Databricks Account, we can add the users / SP to the existing group.
            # before adding users or SP we first need to check if they are available in DB.
            # now read the tmp files and see if we have to add both users and SP or just one of them.

            # Read the temp files.
            filtered_files = filter_tmp_files_by_group_id("groups_users_sps", indv_group_id)
            logging.info(f"The following list of files will be scanned and the members in those "
                         f"files will be created in Databricks Account.")
            logging.info(filtered_files)

            # The filtered_files list will have both the users and SPs or just one of them.
            # Based on the files, we will call the process_files functions to call the creation of
            # user or service principal or call both the functions to create both users and service principals..
            if len(filtered_files) > 0:
                process_files(filtered_files, db_group_to_be_created['displayName'])
            else:
                logging.info(f"This group does not have any members inside, so no action will be taken.")
        else:
            logging.info(f"The group: {db_group_to_be_created['displayName']} is not present in Databricks. "
                         f"So we will now create this group in Databricks Account.")

            # Now creating Databricks Account Group
            # db_account_grp = create_db_account_group(db_group_to_be_created['displayName'])
            # if db_account_grp:

            # time.sleep(10)
            # if the group is Not present in Databricks Account, then create the group first,
            # then add the users/SP to the newly created group.
            # before adding users or SP we first need to check if they are available in DB.
            # now read the tmp files and see if we have to add both users and SP or just one of them.
            filtered_files = filter_tmp_files_by_group_id("groups_users_sps", indv_group_id)
            logging.info(f"The following list of files will be scanned and the members in those "
                         f"files will be created in Databricks Account.")
            logging.info(filtered_files)

            # The filtered_files list will have both the users and SPs or just one of them.
            # Based on the files, we will call the process_files functions to call the creation of
            # user or service principal or call both the functions to create both users and service principals..
            if len(filtered_files) > 0:
                process_files(filtered_files, db_group_to_be_created['displayName'])
            else:
                logging.info(f"This group does not have any members inside, so no action will be taken.")
