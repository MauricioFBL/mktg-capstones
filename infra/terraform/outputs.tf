# output "glue_job_name" {
#   value = module.glue_job.job_name
# }

# output "lambda_function_name" {
#   value = module.lambda.function_name
# }

output "glue_role_name" {
  description = "Name of the IAM role used by Glue"
  value       = aws_iam_role.glue_role.name
}

output "glue_role_arn" {
  description = "ARN of the IAM role used by Glue"
  value       = aws_iam_role.glue_role.arn
}
