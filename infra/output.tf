output "bucket_bronze_name" {
  value       = aws_s3_bucket.bronze.id
  description = "Nome real do bucket bronze na AWS, usado para a ingestão dos dados."
  sensitive   = false
}