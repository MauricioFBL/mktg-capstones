terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
module "glue_jobs" {
  source = "./glue"

  # Pasa variables si las necesita:
  s3_bucket = var.s3_bucket
  glue_role = var.glue_role
}
