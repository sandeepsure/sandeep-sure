provider "aws" {
  region = "us-east-2"
}

# Create Glue Catalog Database
resource "aws_glue_catalog_database" "etl_db" {
  name = "sandeep_etl_db"
}

# Create IAM Role for Glue
resource "aws_iam_role" "glue_service_role" {
  name = "glue_service_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

# Attach policy to IAM Role
resource "aws_iam_role_policy" "glue_policy" {
  name = "glue-s3-access-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::sandy-1947",
          "arn:aws:s3:::sandy-1947/*"
        ]
      },
      {
        Effect = "Allow",
        Action = ["glue:*"],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = ["logs:*"],
        Resource = "*"
      }
    ]
  })
}

# Create Glue Job
resource "aws_glue_job" "etl_job" {
  name     = "sandeep-etl-job"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://sandy-1947/etl_script.py"  
    python_version  = "3"
  }

  glue_version        = "4.0"
  max_retries         = 0
  number_of_workers   = 2
  worker_type         = "G.1X"
  timeout             = 10
  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"                 = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"     = "true"
  }
}


# Create Glue Crawler
resource "aws_glue_crawler" "etl_crawler" {
  name = "sandeep-etl-crawler"
  role = aws_iam_role.glue_service_role.arn

  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "s3://sandy-1947"
  }

  schedule = "cron(0 12 * * ? *)"  # Daily at 12:00 UTC

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}