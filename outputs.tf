# infrastructure/outputs.tf

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint (private – only accessible inside VPC)"
  value       = aws_db_instance.postgres.address
  sensitive   = false
}

output "s3_bucket_name" {
  description = "S3 bucket for EMR logs and Parquet output"
  value       = aws_s3_bucket.pipeline.bucket
}

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = aws_emr_cluster.main.id
}

output "secret_name" {
  description = "Secrets Manager secret name for DB credentials"
  value       = aws_secretsmanager_secret.db_creds.name
}

output "emr_primary_dns" {
  description = "EMR primary node private DNS (SSH via bastion if needed)"
  value       = aws_emr_cluster.main.master_public_dns
}
