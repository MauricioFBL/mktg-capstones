# ----------------------------
# IAM Role for ec2
# ----------------------------
resource "aws_iam_role" "airflow_role" {
  name = "airflow-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "airflow_instance_profile" {
  name = "airflow-ec2-profile"
  role = aws_iam_role.airflow_role.name
}

resource "aws_iam_role_policy" "ssm_read_airflow_admin" {
  name = "AirflowSSMReadAccess"
  role = aws_iam_role.airflow_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowReadAirflowAdminParam",
        Effect = "Allow",
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParameterHistory"
        ],
        Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/airflow/admin"
      },
      {
        Sid      = "AllowKMSDecryptIfEncrypted",
        Effect   = "Allow",
        Action   = ["kms:Decrypt"],
        Resource = "*",
        Condition = {
          StringLike = {
            "kms:ViaService" = "ssm.${var.region}.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "airflow_dag_extended_permissions" {
  name = "AirflowDagCommonActions"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid : "StartAndMonitorGlueJobs",
        Effect : "Allow",
        Action : [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJob",
          "glue:ListJobs"
        ],
        Resource : "*"
      },
      {
        Sid : "RunAthenaQueries",
        Effect : "Allow",
        Action : [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Resource : "*"
      },
      {
        Sid : "MinimalS3Access",
        Effect : "Allow",
        Action : [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource: [
          "arn:aws:s3:::fcorp-data-prod/*",
          "arn:aws:s3:::fcorp-data-prod",
          ]
      },
      {
          Sid : "AllowGetGlueExecutionRole",
          Effect : "Allow",
          Action : ["iam:GetRole"],
          Resource : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/glue-role-${var.environment}"
      },
      {
          Sid    = "AllowGlueCreateDatabase",
          Effect = "Allow",
          Action = [
            "glue:CreateDatabase",
            "glue:UpdateDatabase",
            "glue:GetDatabase",
            "glue:GetDatabases",
            "glue:DeleteDatabase",
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:GetTable",
            "glue:GetTables",
            "glue:DeleteTable",
            "glue:CreatePartition",
            "glue:UpdatePartition",
            "glue:GetPartition",
            "glue:GetPartitions",
            "glue:DeletePartition",
          ],
          Resource = [
            "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
            "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/*",
            "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/*/*",
          ]
        }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_extended" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_dag_extended_permissions.arn
}

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
        Sid : "ListSpecificPrefixes",
        Effect : "Allow",
        Action : [
          "s3:ListBucket"
        ],
        Resource : "arn:aws:s3:::fcorp-data-prod",
        Condition : {
          StringLike : {
            "s3:prefix" : [
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
        Sid : "ReadWriteAllowedPrefixes",
        Effect : "Allow",
        Action : [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource : [
          "arn:aws:s3:::fcorp-data-prod/raw/*",
          "arn:aws:s3:::fcorp-data-prod/staging/*",
          "arn:aws:s3:::fcorp-data-prod/consumption/*",
          "arn:aws:s3:::fcorp-data-prod/scripts/glue/*",
          "arn:aws:s3:::fcorp-data-prod/logs/*",
          "arn:aws:s3:::fcorp-data-prod/scripts/*",
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
