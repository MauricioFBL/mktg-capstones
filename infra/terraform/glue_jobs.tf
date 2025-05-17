# resource "aws_s3_object" "job_a_script" {
#   bucket = var.glue_bucket
#   key    = "scripts/glue/job_a.py"
#   source = "${path.module}/../../../../scripts/glue_jobs/job_a.py"
#   etag   = filemd5("${path.module}/../../../../scripts/glue_jobs/job_a.py")
# }

# resource "aws_glue_job" "job_a" {
#   name     = "job_a"
#   role_arn = aws_iam_role.glue_role.arn

#   command {
#     name            = "glueetl"
#     script_location = "s3://${var.glue_bucket}/scripts/glue/job_a.py"
#     python_version  = "3"
#   }

#   glue_version = "3.0"
#   max_capacity = 2
# }

# ----------------------------
# AWS Glue spark job infrastructure
# ----------------------------

# Loads Glue script to S3
resource "aws_s3_object" "data_transformation_social_media_script" {
  bucket = var.glue_bucket
  key    = "scripts/glue/data-transformation-social-media.py"
  source = "../../jobs/social-media-mktg/social-media-big-tbl.py"
  etag   = filemd5("../../jobs/social-media-mktg/social-media-big-tbl.py")

  tags = {
    Environment = var.environment
  }
}

# Define Glue job
resource "aws_glue_job" "data_transformation_social_media" {
  name     = "data-transformation-social-media"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_bucket}/scripts/glue/data-transformation-social-media.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  description       = "Transforms social media campaign, ad, and daily data into a unified table"

  tags = {
    Project     = "marketing-analytics"
    Environment = var.environment
  }
}

# ----------------------------
# AWS Glue python shell job infrastructure
# ----------------------------
resource "aws_s3_object" "marketing_data_simulation" {
  bucket = var.glue_bucket
  key    = "scripts/python_shell/sample-meta-gen.py"
  source = "../../jobs/social-media-mktg/sample-meta-gen.py"
  etag   = filemd5("../../jobs/social-media-mktg/sample-meta-gen.py")
}

resource "aws_glue_job" "simulate_marketing_data" {
  name     = "sdata-ingestion-social-media"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${var.glue_bucket}/scripts/python_shell/sample-meta-gen.py"
    python_version  = "3.9"
  }

  glue_version = "3.0"
  max_capacity = 0.0625

  default_arguments = {
    "--TempDir" = "s3://${var.glue_bucket}/temp/"
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}
# -------------- SOCIAL MEDIA FINAL JOB--------------
resource "aws_s3_object" "data_consumption_social_media_meta_script" {
  bucket = var.glue_bucket
  key    = "scripts/glue/data-consumption-meta-month.py"
  source = "../../jobs/social-media-mktg/data-consumption-meta-month.py"
  etag   = filemd5("../../jobs/social-media-mktg/data-consumption-meta-month.py")

  tags = {
    Environment = var.environment
  }
}

# Creación del Glue Job
resource "aws_glue_job" "data_consumption_social_media_meta" {
  name     = "data-consumption-social-media-meta"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_bucket}/scripts/glue/data-consumption-meta-month.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  description       = "Summarizes social media campaign, ad, and daily data into a monthly table"
  max_retries       = 1
  default_arguments = {
    "--TempDir"     = "s3://${var.glue_bucket}/temp/"
    "--INPUT_PATH"  = "s3://${var.glue_bucket}/staging/marketing/social_media/src=meta/meta_daily.csv/"
    "--OUTPUT_PATH" = "s3://${var.glue_bucket}/consumption/marketing/social_media/meta_monthly/"
  }


  tags = {
    Project     = "marketing-analytics"
    Environment = var.environment
  }
}
