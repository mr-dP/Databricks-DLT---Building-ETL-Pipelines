# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

#
# 1. App registrations | TestDBSYN17
#           Application (client) ID             ########-####-####-####-############
#           Directory (tenant) ID               ########-####-####-####-############

# 2. TestDBSYN17 | Certificates & secrets
#           Value                               #####~##################################
#           Secret ID                           ########-####-####-####-############

# 3. 0xxx0storageaccountadls | Access Control (IAM)
#           Role assignments    |   Add     |       Add role assignment
#           Storage Blob Data Contributor
#           Assign access to        User, group, or service principal
#           Members          Select members           TestDBSYN17

# 4. Key vaults         Create a key vault
#           xxx0-keyvault-databricks | Secrets          Generate/Import
#           Properties
#           Vault URI       https://####-########-############.vault.azure.net/
#           Resource ID     /subscriptions/########-####-####-####-############/resourceGroups/#####_Resoure_Group_###/providers/Microsoft.KeyVault/vaults/####-########-##########

# 5. Databricks Create Secret Scope
#       https://adb-xxxxzzzzoooooooo.**.azuredatabricks.net/?o=****************#secrets/createScope
#       Scope Name      (Name of Azure Key Vault)       ####-########-############
#       Manage Principal        All Users               (All users can manage the secret scopes)
#       Azure Key Vault DNS Name            [Vault URI] from Step 4
#       Resource ID                         [Resource ID] from Step 4

# COMMAND ----------

help(dbutils.secrets)

# COMMAND ----------

scope_name = dbutils.secrets.listScopes()[0][0]
print(scope_name)

tenant_id = dbutils.secrets.get(scope=scope_name, key="tenant-id")
print(tenant_id)

application_id = dbutils.secrets.get(scope=scope_name, key="application-id")
print(application_id)

secret = dbutils.secrets.get(scope=scope_name, key="secret")
print(secret)

# COMMAND ----------

container_name = "raw"
storage_account_name = "0xxx0storageaccountadls"
mount_name = "/mnt/raw"

# COMMAND ----------

# dbutils.fs.unmount(mount_name)

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point=mount_name,
    extra_configs=configs,
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw"))

# COMMAND ----------

# display(dbutils.fs.ls("dbfs:/mnt/raw/deep_dive/"))

# COMMAND ----------

container_name = "datalake"
mount_name = "/mnt/datalake"

# COMMAND ----------

dbutils.fs.unmount(mount_name)

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point=mount_name,
    extra_configs=configs,
)

# COMMAND ----------

dbutils.fs.ls(mount_name)

# COMMAND ----------


