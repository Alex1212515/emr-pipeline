# ============================================================
# infrastructure/main.tf
# ============================================================
# Provisions the core AWS resources for the EMR pipeline:
#   - VPC with public + private subnets
#   - RDS PostgreSQL (private subnet)
#   - EMR Cluster (private subnet, can reach RDS via SG rule)
#   - Secrets Manager secret for DB credentials
#   - IAM roles (EMR EC2 profile + service role)
#   - S3 bucket for EMR logs and output
#
# Design principles:
#   - RDS is in private subnets ONLY – never directly reachable
#     from the public internet.
#   - EMR nodes reach RDS via Security Group rule (SG-to-SG),
#     not by opening port 5432 to 0.0.0.0/0.
#   - DB credentials live in Secrets Manager; EMR IAM role has
#     secretsmanager:GetSecretValue permission scoped to this
#     specific secret ARN only (least privilege).
#   - All S3 traffic uses VPC Gateway Endpoint (no NAT required
#     for S3, no data traverses the public internet).
# ============================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "emr-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ── Random suffix for globally-unique resource names ─────────────────────────
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name_prefix = "emr-pipeline-${var.environment}"
  suffix      = random_id.suffix.hex
}


# ════════════════════════════════════════════════════════════
# VPC & Networking
# ════════════════════════════════════════════════════════════

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = { Name = "${local.name_prefix}-vpc" }
}

# Public subnets (2 AZs) – for NAT gateway only
resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags = { Name = "${local.name_prefix}-public-${count.index + 1}" }
}

# Private subnets (2 AZs) – RDS and EMR nodes live here
resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags = { Name = "${local.name_prefix}-private-${count.index + 1}" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name_prefix}-igw" }
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  depends_on    = [aws_internet_gateway.igw]
  tags          = { Name = "${local.name_prefix}-nat" }
}

# Route tables
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
  tags = { Name = "${local.name_prefix}-rt-public" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }
  tags = { Name = "${local.name_prefix}-rt-private" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# S3 VPC Gateway Endpoint – keeps S3 traffic off the public internet
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = { Name = "${local.name_prefix}-s3-endpoint" }
}

# Secrets Manager VPC Interface Endpoint – EMR nodes call SM without NAT
resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${local.name_prefix}-sm-endpoint" }
}

data "aws_availability_zones" "available" { state = "available" }


# ════════════════════════════════════════════════════════════
# Security Groups
# ════════════════════════════════════════════════════════════

resource "aws_security_group" "rds" {
  name        = "${local.name_prefix}-sg-rds"
  description = "Allow inbound PostgreSQL from EMR nodes only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "PostgreSQL from EMR"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.emr_primary.id, aws_security_group.emr_core.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${local.name_prefix}-sg-rds" }
}

resource "aws_security_group" "emr_primary" {
  name        = "${local.name_prefix}-sg-emr-primary"
  description = "EMR primary node security group"
  vpc_id      = aws_vpc.main.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${local.name_prefix}-sg-emr-primary" }
}

resource "aws_security_group" "emr_core" {
  name        = "${local.name_prefix}-sg-emr-core"
  description = "EMR core/task node security group"
  vpc_id      = aws_vpc.main.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${local.name_prefix}-sg-emr-core" }
}

resource "aws_security_group" "vpc_endpoints" {
  name        = "${local.name_prefix}-sg-vpce"
  description = "Allow HTTPS from VPC to interface endpoints"
  vpc_id      = aws_vpc.main.id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${local.name_prefix}-sg-vpce" }
}


# ════════════════════════════════════════════════════════════
# RDS PostgreSQL
# ════════════════════════════════════════════════════════════

resource "aws_db_subnet_group" "main" {
  name       = "${local.name_prefix}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id
  tags       = { Name = "${local.name_prefix}-db-subnet-group" }
}

resource "random_password" "db_password" {
  length  = 24
  special = false   # avoid chars that break JDBC URL parsing
}

resource "aws_db_instance" "postgres" {
  identifier        = "${local.name_prefix}-rds-${local.suffix}"
  engine            = "postgres"
  engine_version    = "15.4"
  instance_class    = var.rds_instance_class
  allocated_storage = 20
  storage_encrypted = true

  db_name  = "ordersdb"
  username = "dbadmin"
  password = random_password.db_password.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false   # ← private only

  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 7
  multi_az                = false  # set true in prod

  tags = { Name = "${local.name_prefix}-rds" }
}


# ════════════════════════════════════════════════════════════
# Secrets Manager
# ════════════════════════════════════════════════════════════

resource "aws_secretsmanager_secret" "db_creds" {
  name                    = "emr-pipeline/rds/credentials"
  description             = "RDS credentials for EMR pipeline"
  recovery_window_in_days = 0   # immediate delete on destroy (dev)
}

resource "aws_secretsmanager_secret_version" "db_creds" {
  secret_id = aws_secretsmanager_secret.db_creds.id
  secret_string = jsonencode({
    host     = aws_db_instance.postgres.address
    port     = 5432
    dbname   = "ordersdb"
    username = "emr_reader"          # read-only app user (created in post-setup)
    password = random_password.db_password.result
  })
}


# ════════════════════════════════════════════════════════════
# S3 Bucket (logs + output)
# ════════════════════════════════════════════════════════════

resource "aws_s3_bucket" "pipeline" {
  bucket        = "${local.name_prefix}-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "pipeline" {
  bucket                  = aws_s3_bucket.pipeline.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload bootstrap script to S3
resource "aws_s3_object" "bootstrap" {
  bucket = aws_s3_bucket.pipeline.id
  key    = "bootstrap/bootstrap_emr.sh"
  source = "${path.module}/../module2_emr/bootstrap_emr.sh"
  etag   = filemd5("${path.module}/../module2_emr/bootstrap_emr.sh")
}

# Upload PySpark job to S3
resource "aws_s3_object" "pyspark_job" {
  bucket = aws_s3_bucket.pipeline.id
  key    = "jobs/pyspark_extract.py"
  source = "${path.module}/../module2_emr/pyspark_extract.py"
  etag   = filemd5("${path.module}/../module2_emr/pyspark_extract.py")
}


# ════════════════════════════════════════════════════════════
# IAM – EMR Service Role & EC2 Instance Profile
# ════════════════════════════════════════════════════════════

resource "aws_iam_role" "emr_service" {
  name = "${local.name_prefix}-emr-service-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2" {
  name = "${local.name_prefix}-emr-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Least-privilege inline policy: S3 (scoped to bucket) + Secrets Manager (scoped to secret)
resource "aws_iam_role_policy" "emr_ec2_policy" {
  name = "emr-pipeline-access"
  role = aws_iam_role.emr_ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3BucketAccess"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.pipeline.arn,
          "${aws_s3_bucket.pipeline.arn}/*"
        ]
      },
      {
        Sid      = "SecretsManagerReadOnly"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [aws_secretsmanager_secret.db_creds.arn]
      },
      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${local.name_prefix}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2.name
}


# ════════════════════════════════════════════════════════════
# EMR Cluster
# ════════════════════════════════════════════════════════════

resource "aws_emr_cluster" "main" {
  name          = "${local.name_prefix}-cluster"
  release_label = "emr-6.15.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id                         = aws_subnet.private[0].id
    emr_managed_master_security_group = aws_security_group.emr_primary.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
  }

  master_instance_group {
    instance_type = var.emr_master_instance_type
  }

  core_instance_group {
    instance_count = var.emr_core_instance_count
    instance_type  = var.emr_core_instance_type
  }

  bootstrap_action {
    name = "install-jdbc-driver"
    path = "s3://${aws_s3_bucket.pipeline.bucket}/bootstrap/bootstrap_emr.sh"
  }

  log_uri      = "s3://${aws_s3_bucket.pipeline.bucket}/emr-logs/"
  service_role = aws_iam_role.emr_service.arn

  configurations_json = jsonencode([
    {
      Classification = "spark-env"
      Configurations = [{
        Classification = "export"
        Properties = {
          AWS_REGION = var.aws_region
        }
      }]
    },
    {
      Classification = "spark"
      Properties = {
        "maximizeResourceAllocation" = "true"
      }
    }
  ])

  tags = { Name = "${local.name_prefix}-emr" }
  depends_on = [aws_s3_object.bootstrap]
}
