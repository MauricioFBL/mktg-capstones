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
  source = "./glue_job"

  # Pasa variables si las necesita:

  environment = var.environment
  project     = var.project
  region      = var.region
  glue_bucket = var.glue_bucket
}
