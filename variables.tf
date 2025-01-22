variable "subscription_id" {
  description = "The Azure subscription ID."
  type        = string
}

variable "user_email" {
  description = "Email of the Databricks user."
  type        = string
}

variable "service_principal_object_id" {
  description = "The object ID of the service principal"
  type        = string
}
