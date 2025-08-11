output "glue_job_name" {
  value = aws_glue_job.etl_job.name
}

output "iam_role_arn" {
  value = aws_iam_role.glue_service_role.arn
}

output "crawler_name" {
  value = aws_glue_crawler.etl_crawler.name
}