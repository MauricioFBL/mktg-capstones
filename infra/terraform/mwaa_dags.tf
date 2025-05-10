# resource "aws_s3_object" "dag_sales_report" {
#   bucket = var.mwaa_bucket
#   key    = "dags/sales_report.py"
#   source = "${path.module}/../../../../dags/sales_report.py"
#   etag   = filemd5("${path.module}/../../../../dags/sales_report.py")
# }
