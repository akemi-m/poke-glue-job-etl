variable "aws_region" {
  description = "Região AWS utilizada no projeto"
  type        = string
}

variable "bucket_name" {
  description = "Nome do bucket S3 do projeto"
  type        = string
}

variable "glue_role_arn" {
  description = "ARN da IAM Role utilizada pelo AWS Glue"
  type        = string
}

variable "glue_job_name" {
  description = "Nome do Glue Job"
  type        = string
}