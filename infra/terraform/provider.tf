# output "lambda_names" {
#   value = module.lambda_functions.lambda_names
# }

output "glue_roles" {
  value = aws_iam_role.glue_role.arn
}

data "aws_caller_identity" "current" {}
