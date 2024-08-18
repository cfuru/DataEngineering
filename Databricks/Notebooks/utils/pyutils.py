# Databricks notebook source
from pyspark.sql import DataFrame

# COMMAND ----------

class settingsFactory:
  def __init__(
    self, 
    secretScopeName
  ):
    self.secretScopeName = secretScopeName
    self.storageAccountName = dbutils.secrets.get(self.secretScopeName, "dls-blobName")
    self.storageAccountAccessKey = dbutils.secrets.get(secretScopeName, "dls-key")

  def configure_default_azure_storage_access(self) -> None:
      spark.conf.set(
        f"fs.azure.account.key.{self.storageAccountName}.dfs.core.windows.net", 
        self.storageAccountAccessKey)

defaultSettings = settingsFactory(
  secretScopeName = "key-vault-secret"
)
defaultSettings.configure_default_azure_storage_access()
  

# COMMAND ----------


def get_abfss_path(
    storageAccount: str = defaultSettings.storageAccountName,
    container: str = None,
    subfolder: str = None
) -> str:
    abfssRootPath = f"abfss://{container}@{storageAccount}.dfs.core.windows.net"
    if subfolder != None:
        return f"{abfssRootPath}/{subfolder}"
    return abfssRootPath

def get_wasbs_path(
    storageAccount: str = defaultSettings.storageAccountName,
    container: str = None,
    subfolder: str = None
) -> str:
    wasbsRootPath = f"wasbs://{container}@{storageAccount}.blob.core.windows.net"
    if subfolder != None:
        return f"{wasbsRootPath}/{subfolder}"
    return wasbsRootPath

# COMMAND ----------


