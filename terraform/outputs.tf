output "glue_job_name" {
  description = "Nome do Glue Job criado"
  value       = aws_glue_job.pokemon_etl_job.name
}

output "glue_job_arn" {
  description = "ARN do Glue Job criado"
  value       = aws_glue_job.pokemon_etl_job.arn
}

output "script_location" {
  description = "Caminho do script PySpark no S3"
  value       = "s3://${var.bucket_name}/scripts/main.py"
}