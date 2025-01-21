# Resource random_pet is from Terraform used for avoiding name conflicts
resource "random_pet" "prefix" {
  length = 1
}

# Azure Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "${random_pet.prefix.id}-rg-databricks-project"
  location = "Southeast Asia"
}

data "azurerm_client_config" "current" {}

# Azure Data Lake Storage
resource "azurerm_storage_account" "adls" {
  name                     = "${random_pet.prefix.id}datastorage"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

# Storage Containers
resource "azurerm_storage_container" "containers" {
  for_each              = toset(["landing", "medallion", "checkpoints"])
  name                  = each.value
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

# Medallion container directories
resource "azurerm_storage_data_lake_gen2_path" "medallion_directories" {
  for_each           = toset(["bronze", "silver", "gold"])
  path               = each.value
  filesystem_name    = azurerm_storage_container.containers["medallion"].name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}

# Azure Key Vault
resource "azurerm_key_vault" "keyvault" {
  name                = "${random_pet.prefix.id}-keyvault"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku_name            = "standard"
  tenant_id           = data.azurerm_client_config.current.tenant_id

  # Access policy for the current user
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get",
      "List",
      "Delete"
    ]

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete"
    ]
  }
}

# Store storage account key in Key Vault
resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "storageAccountKey"
  value        = azurerm_storage_account.adls.primary_access_key
  key_vault_id = azurerm_key_vault.keyvault.id
}

# Store storage account name in Key Vault
resource "azurerm_key_vault_secret" "storage_account_name" {
  name         = "storageAccountName"
  value        = azurerm_storage_account.adls.name
  key_vault_id = azurerm_key_vault.keyvault.id
}

# Azure Databricks
resource "azurerm_databricks_workspace" "databricks" {
  name                = "${random_pet.prefix.id}-databricks"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "standard"
}

# Create Databricks Secret Scope
resource "databricks_secret_scope" "adbSecretScope" {
  name                     = "adbSecretScope"
  initial_manage_principal = "users"
}

# Store Storage Account Key in Databricks Secret Scope
resource "databricks_secret" "storage_account_key" {
  key          = "storageAccountKey"
  string_value = azurerm_storage_account.adls.primary_access_key
  scope        = databricks_secret_scope.adbSecretScope.name
}

# Store Storage Account Name in Databricks Secret Scope
resource "databricks_secret" "storage_account_name" {
  key          = "storageAccountName"
  string_value = azurerm_storage_account.adls.name
  scope        = databricks_secret_scope.adbSecretScope.name
}

# Create Databricks Cluster
resource "databricks_cluster" "cluster" {
  cluster_name            = "adb-project-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 15
  num_workers             = 0
  spark_conf = {
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  single_user_name        = var.user_email
}

# Define paths to your Databricks notebooks
locals {
  notebooks = [
    {
      path      = "${path.module}/1.storageMount.ipynb",
      dest_path = "/Workspace/Users/${var.user_email}/1.storageMount"
    },
    {
      path      = "${path.module}/2.ingestBronze.ipynb",
      dest_path = "/Workspace/Users/${var.user_email}/2.ingestBronze"
    },
    {
      path      = "${path.module}/3.transformSilver.ipynb",
      dest_path = "/Workspace/Users/${var.user_email}/3.transformSilver"
    },
    {
      path      = "${path.module}/4.transformGold.ipynb",
      dest_path = "/Workspace/Users/${var.user_email}/4.transformGold"
    },
    {
      path      = "${path.module}/5.loadDelta.ipynb",
      dest_path = "/Workspace/Users/${var.user_email}/5.loadDelta"
    },
    {
      path      = "${path.module}/6.pipelineTest.ipynb",
      dest_path = "/Workspace/Users/${var.user_email}/6.pipelineTest"
    }
  ]
}

# Upload Databricks notebooks
resource "databricks_notebook" "notebooks" {
  for_each = { for n in local.notebooks : n.path => n }
  path     = each.value.dest_path
  source   = each.key
  language = "PYTHON"
}

# Create Databricks job
resource "databricks_job" "etl_pipeline" {
  name                = "Coffee Shop ETL Pipeline"
  timeout_seconds     = 0
  max_concurrent_runs = 1

  task {
    task_key = "mount_storage"
    run_if   = "ALL_SUCCESS"
    notebook_task {
      notebook_path = databricks_notebook.notebooks["databricks/notebooks/1.storageMount.ipynb"].path
      source        = "WORKSPACE"
    }
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = 0
  }

  task {
    task_key = "ingest_bronze"
    run_if   = "ALL_SUCCESS"
    depends_on {
      task_key = "mount_storage"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebooks["databricks/notebooks/2.ingestBronze.ipynb"].path
      source        = "WORKSPACE"
    }
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = 0
  }

  task {
    task_key = "transform_silver"
    run_if   = "ALL_SUCCESS"
    depends_on {
      task_key = "ingest_bronze"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebooks["databricks/notebooks/3.transformSilver.ipynb"].path
      source        = "WORKSPACE"
    }
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = 0 
  }

  task {
    task_key = "transform_gold"
    run_if   = "ALL_SUCCESS"
    depends_on {
      task_key = "transform_silver"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebooks["databricks/notebooks/4.transformGold.ipynb"].path
      source        = "WORKSPACE"
    }
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = 0 
  }

  task {
    task_key = "load_delta_tables"
    run_if   = "ALL_SUCCESS"
    depends_on {
      task_key = "transform_gold"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebooks["databricks/notebooks/5.loadDelta.ipynb"].path
      source        = "WORKSPACE"
    }
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = 0
  }
}