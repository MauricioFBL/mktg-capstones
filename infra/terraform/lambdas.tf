# module "lambda_functions" {
#   source = "./modules/lambda_multi"

#   functions = {
#     extractor_a = {
#       zip_file = "extractor_a.zip"
#       handler  = "lambda_function.lambda_handler"
#       runtime  = "python3.9"
#     }
#   }

#   source_dir  = "${path.module}/../../../../build"
#   s3_bucket   = var.lambda_bucket
#   role_arn    = aws_iam_role.lambda_role.arn
#   environment = var.environment
# }
