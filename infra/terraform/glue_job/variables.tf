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

variable "project" {
  description = "Project namespace prefix"
  default     = "fcorp"
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue Jobs"
  type        = string
}
