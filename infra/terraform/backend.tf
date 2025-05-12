terraform {
  backend "s3" {
    bucket         = "fcorp-tfstate"
    key            = "terraform/fcorp-data-prod.tfstate"
    region         = "us-east-1"
    dynamodb_table = "fcorp-tf-lock"
    encrypt        = true
  }
}
