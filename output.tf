output "adls_access_key" {
  value     = azurerm_storage_account.adls.primary_access_key
  sensitive = true
}

output "landing_container_name" {
  value = azurerm_storage_container.containers["landing"].name
}

output "storage_account_name" {
  value = azurerm_storage_account.adls.name
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.databricks.workspace_id
}
