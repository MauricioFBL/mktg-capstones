# ----------------------------
# IAM Role for AWS Glue
# ----------------------------
resource "aws_iam_role" "glue_role" {
  name = "glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

# ----------------------------
# Managed Policies (predefinidas por AWS)
# ----------------------------

# Glue core role
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Athena access
resource "aws_iam_role_policy_attachment" "athena_full_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

# Glue console/job orchestration access
resource "aws_iam_role_policy_attachment" "glue_console_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

# ----------------------------
# Custom Policy: Limited S3 Prefix Access
# ----------------------------
resource "aws_iam_policy" "glue_s3_prefix_policy" {
  name        = "glue-s3-prefix-policy"
  description = "Glue access to specific S3 prefixes inside fcorp-data-prod"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid: "ListSpecificPrefixes",
        Effect: "Allow",
        Action: [
          "s3:ListBucket"
        ],
        Resource: "arn:aws:s3:::fcorp-data-prod",
        Condition: {
          StringLike: {
            "s3:prefix": [
              "raw/*",
              "staging/*",
              "consumption/*",
              "scripts/glue/*",
              "logs/*"
            ]
          }
        }
      },
      {
        Sid: "ReadWriteAllowedPrefixes",
        Effect: "Allow",
        Action: [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource: [
          "arn:aws:s3:::fcorp-data-prod/raw/*",
          "arn:aws:s3:::fcorp-data-prod/staging/*",
          "arn:aws:s3:::fcorp-data-prod/consumption/*",
          "arn:aws:s3:::fcorp-data-prod/scripts/glue/*",
          "arn:aws:s3:::fcorp-data-prod/logs/*"
        ]
      }
    ]
  })
}

# Attach custom policy to the role
resource "aws_iam_role_policy_attachment" "glue_custom_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_prefix_policy.arn
}
