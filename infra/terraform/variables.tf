variable "region" {
  default = "us-east-1"
}
# variables.tf
variable "environment" {
  description = "Ambiente de trabajo (dev, prod, etc)"
  default     = "prod"
}

variable "glue_bucket" {
  description = "Nombre del bucket donde están los Glue scripts"
  type        = string
}

variable "lambda_bucket" {
  description = "Nombre del bucket donde están las funciones Lambda"
  type        = string
}

variable "mwaa_bucket" {
  description = "Nombre del bucket donde están los DAGs de Airflow"
  type        = string
}

variable "project" {
  description = "Project namespace prefix"
  default     = "fcorp"
}

variable "public_key" {
  description = "SSH public key"
  type        = string
}

variable "db_password" {
  description = "SSH public key"
  type        = string
  default     = "airflow123"
}
