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

# Limitations
1. Only supports Azure Databricks (AWS Not supported).
2. Sometimes when adding users or service principals to Databricks groups, the users/SP's may not get added. Just re-run the script once again to add the missing members.
