output "bucket_bronze_name" {
  value       = aws_s3_bucket.bronze.id
  description = "Nome real do bucket bronze na AWS"
}

output "bucket_silver_name" {
  value       = aws_s3_bucket.silver.id
  description = "Nome real do bucket silver na AWS"
}

output "bucket_gold_name" {
  value       = aws_s3_bucket.gold.id
  description = "Nome real do bucket gold na AWS"
}