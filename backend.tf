# Terraform backend configuration 
terraform {
  backend "azurerm" {
    resource_group_name  = "terraform-backend-rg"
    storage_account_name = "tfstateprojectstorage"
    container_name       = "dev-tfstate"
    key                  = "terraform.tfstate"
  }
}