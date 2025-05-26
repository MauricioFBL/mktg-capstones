# ----------------------------
# AWS Glue python shell job infrastructure -> 01-simulate-multiples-tables
# ----------------------------

# Loads Glue script to S3
resource "aws_s3_object" "aws_load_01_simulate_multiples_tables" {
  bucket = var.glue_bucket
  key    = "scripts/python_shell/01-simulate-multiples-tables.py"
  source = "../../jobs/policies/01-simulate-multiples-tables.py"
  etag   = filemd5("../../jobs/policies/01-simulate-multiples-tables.py")

  tags = {
    Environment = var.environment
    Project     = var.project
  }
}

# Define Glue job
resource "aws_glue_job" "el_01_simulate_multiples_tables" {
  name = "01-simulate-multiples-tables"
  role_arn = var.glue_role_arn

  command {
    name            = "pythonshell"
    script_location = "s3://${var.glue_bucket}/scripts/python_shell/01-simulate-multiples-tables.py"
    python_version  = "3.9"
  }

  glue_version = "3.0"
  max_capacity = 0.0625
  max_retries  = 1
  description  = "Script to create 4 tables from 1 file to simulate data from several sources"


  default_arguments = {
    "--TempDir" = "s3://${var.glue_bucket}/temp/"
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}


# ----------------------------
# AWS Glue spark job infrastructure -> 02-data-clean-big-table
# ----------------------------

# Loads Glue script to S3
resource "aws_s3_object" "aws_load_02_data_clean_big_table" {
  bucket = var.glue_bucket
  key    = "scripts/glue/02-data-clean-big-table.py"
  source = "../../jobs/policies/02-data-clean-big-table.py"
  etag   = filemd5("../../jobs/policies/02-data-clean-big-table.py")

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

# Define Glue job
resource "aws_glue_job" "el_02_data_clean_big_table" {
  name = "02-data-clean-big-table"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_bucket}/scripts/glue/02-data-clean-big-table.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  description       = "Glue job to get policies data from several files, clean them and create a one big table"
  max_retries       = 1

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}


# ----------------------------
# AWS Glue spark job infrastructure -> 03-policies-consumption
# ----------------------------

# Loads Glue script to S3
resource "aws_s3_object" "aws_load_03_policies_consumption" {
  bucket = var.glue_bucket
  key    = "scripts/glue/03-policies-consumption.py"
  source = "../../jobs/policies/03-policies-consumption.py"
  etag   = filemd5("../../jobs/policies/03-policies-consumption.py")

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

# Define Glue job
resource "aws_glue_job" "el_03_policies_consumption" {
  name = "03-policies-consumption"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_bucket}/scripts/glue/03-policies-consumption.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  description       = "Glue job to get policies data from clean stage, applies transformations and write on consumption stage"
  max_retries       = 1

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}
