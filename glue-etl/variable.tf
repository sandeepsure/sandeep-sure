variable "aws_region" {
  default = "us-east-2"
}

variable "glue_job_name" {
  default = "sandeep-etl-job"
}

variable "script_s3_path" {
  default = "s3://sandy-1947/etl_script.py"
}