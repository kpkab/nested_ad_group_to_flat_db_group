# nested_ad_group_to_flat_db_group
Read all complex nested Azure AD (EntraID) groups and create them as a single group with all group members in Databricks.

# Azure Permissions.
In order to run this, you will need the following from Azure:
1. client_id
2. client_secret
3. tenant_id

# Databricks Permissions.
You will need the following from your Databricks Account.
1. SCIM token
2. SCIM URL
3. Databricks Account Number
4. Azure Databricks Host URL

Apart from these, you will need read only permissions on your Azure Active Directory.

# Before running
1. Clone this repo.
2. Create 2 folders 1) groups_users_sps and 2) logs
3. Enter the Azure AD group names or Azure AD group ids or Azure AD user names in the groups_to_sync.json file.

# How to run
Once the above setup is complete, just run the main.py script. It will read the entires in the groups_to_sync.json file and 
create those in your Databricks Account.

# Scenarios covered
1. Sync Nested AD group from Azure to Databricks (group, users and service principals are not present in Databricks) - In this case, we havea  nested Azure AD group with members (users or service principals) in several layers. In this case, the script will create a Databricks Account group. The Databricks group will have the same name as the Azure AD top-level group with all the members from the nested group assigned to this one group in Databricks account.
2. Sync Nested AD group from Azure to Databricks, where some users or service principal already exists in Databricks Account - This process follows the same flow as described above, but the users that already exists in Databricks Account will not be re-created (they will be ignored). But these existing users will be added to the newly created group.
3. Sync Nested AD group from Azure to Databricks, where the AD group already exists in Databricks with some members - In this case, since the group is already present in Databricks, the existing group with existing members will be retained, and only the new members will be added.
4. Create Users in Databricks Account - In order to create new users in Databricks account, you can use the groups_to_sync.json file and list the new users under "users" key. This will create the users in Databricks Account, only if those users exists in Azure AD. If the user is not in Azure AD then the user will not be created in Databricks Account.
5. Sync groups using group names - If you know the AD group names, you can mention them as a list in the groups_to_sync.json file and all the group names will be sync'd. If a particular group is not present in Azure AD that group will be ignored (only groups in Azure AD will be created in Databricks Account).
6. Sync groups using group id - sometimes, your AD group names may have special characters, in those cases, if the script fails (because of the presence of special characters), then use the group ID from Azure AD. The script internally uses the group ID to get the group and memeber detials.
7. Sync one or multiple number of groups - The groups_to_sync.json file takes the group names, group id and users (names) as a list, so you can sync multiple items at the same time.
8. Sync groups and users at the same time - You can mention group names or group ID and usernames for the same run. 

# Logs
Everytime you run the script, it will create a log file in the logs directory. The log file uses timestamp as part of the name, so you can get the latest logs using the most recent timestamp. The log does show the usernames, group names and groupIDs for better redability. You can comment these if needed.

# Limitations
1. Only supports Azure Databricks (AWS Not supported).
2. Sometimes when adding users or service principals to Databricks groups, the users/SP's may not get added. Just re-run the script once again to add the missing members.
