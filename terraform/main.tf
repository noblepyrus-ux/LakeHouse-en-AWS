# ==========================================
# PROVIDER DE AWS
# ==========================================
terraform {
  required_version = ">= 1.0.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
  
  default_tags {
    tags = {
      Project     = "lakehouse-andres"
      Environment = "dev"
      Owner       = "andres-parada"
      Course      = "data-lakehouse"
      ManagedBy   = "terraform"
    }
  }
}

# ==========================================
# BUCKETS S3 - ARQUITECTURA MEDALLION
# ==========================================

# Bucket para datos crudos (CSV, JSON)
resource "aws_s3_bucket" "landing" {
  bucket = "lakehouse-andres-2026-landing"
}

# Bucket para datos crudos catalogados (Bronze)
resource "aws_s3_bucket" "bronze" {
  bucket = "lakehouse-andres-2026-bronze"
}

# Bucket para datos limpios y tipados (Silver - Iceberg)
resource "aws_s3_bucket" "silver" {
  bucket = "lakehouse-andres-2026-silver"
}

# Bucket para modelo estrella (Gold - Data Warehouse)
resource "aws_s3_bucket" "gold" {
  bucket = "lakehouse-andres-2026-gold"
}

# Bucket para resultados de Athena
resource "aws_s3_bucket" "athena_results" {
  bucket = "lakehouse-andres-2026-athena-results"
}

# Bucket temporal para Glue Jobs
resource "aws_s3_bucket" "temp" {
  bucket = "lakehouse-andres-2026-temp"
}

# ==========================================
# 3. IAM - ROL PARA AWS GLUE
# ==========================================
# Glue necesita permisos para leer S3 y escribir en el Data Catalog.

resource "aws_iam_role" "glue_service_role" {
  name = "lakehouse-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Adjuntar política oficial de servicio para Glue
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Adjuntar acceso completo a S3 (para que pueda leer landing y escribir en bronze/silver/gold)
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ==========================================
# 4. GLUE DATA CATALOG & CRAWLER
# ==========================================

# Base de datos en el Glue Catalog para la capa Bronze
resource "aws_glue_catalog_database" "bronze_db" {
  name = "lakehouse_bronze_db"
}

# Crawler que escanea la carpeta 'landing' y crea las tablas en 'bronze_db'
resource "aws_glue_crawler" "landing_crawler" {
  name          = "lakehouse-landing-crawler"
  role          = aws_iam_role.glue_service_role.name
  database_name = aws_glue_catalog_database.bronze_db.name
  
  # Apunta a la raíz del bucket landing
  s3_target {
    path = "s3://${aws_s3_bucket.landing.id}/"
  }

  # Política de esquema: si cambia el CSV, actualiza la tabla en lugar de borrarla
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# ==========================================
# 5. PRESUPUESTO (BUDGET) - REGLA DE ORO
# ==========================================
# El PDF exige alerta de 5 USD. Si no está en Terraform, no cuenta.

resource "aws_budgets_budget" "project_budget" {
  name         = "lakehouse-budget-5usd"
  budget_type  = "COST"
  limit_amount = "5"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Notificación al 80% del presupuesto (4 USD)
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["andres.parada@example.com"] # ⚠️ CAMBIA ESTO POR TU CORREO REAL
  }

  # Notificación al 100% del presupuesto (5 USD)
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["andres.parada@example.com"] # ⚠️ CAMBIA ESTO POR TU CORREO REAL
  }
}

# ==========================================
# 6. PERMISOS PARA EL USUARIO DE DESPLIEGUE
# ==========================================
# Referenciamos el usuario que ya existe
data "aws_iam_user" "lakehouse_user" {
  user_name = "lakehouse-user"
}

# Le otorgamos permisos de Administrador mediante Terraform 
# (Necesario para que Terraform pueda crear roles IAM, Glue y Budgets)
resource "aws_iam_user_policy_attachment" "lakehouse_user_admin" {
  user       = data.aws_iam_user.lakehouse_user.user_name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

# ============================================================
# GLUE JOBS CONFIGURATION (Scripts & Jobs)
# ============================================================
# NOTA: El IAM Role 'glue_service_role' ya fue creado arriba en este archivo.
# Aquí solo agregamos la política extra, la subida de scripts y los Jobs.

# 1. Política de permisos extra para Glue (Athena, Logs)
resource "aws_iam_role_policy" "glue_extra_policy" {
  name = "lakehouse-glue-extra-policy"
  role = aws_iam_role.glue_service_role.id # Referencia al role que ya existe arriba

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# 2. Subir scripts de Python a S3 (para que Glue los ejecute)
resource "aws_s3_object" "glue_script_staging" {
  bucket = aws_s3_bucket.temp.id
  key    = "scripts/job_staging_ventas.py"
  source = "${path.module}/../jobs/job_staging_ventas.py"
}

resource "aws_s3_object" "glue_script_silver" {
  bucket = aws_s3_bucket.temp.id
  key    = "scripts/jobs_silver_ventas.py"
  source = "${path.module}/../jobs/jobs_silver_ventas.py"
}

resource "aws_s3_object" "glue_script_gold" {
  bucket = aws_s3_bucket.temp.id
  key    = "scripts/jobs_gold_ventas.py"
  source = "${path.module}/../jobs/jobs_gold_ventas.py"
}

# 3. Glue Job 1: Staging (Spark - CSV -> Parquet)
resource "aws_glue_job" "staging_job" {
  name     = "lakehouse-staging-job"
  role_arn = aws_iam_role.glue_service_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.temp.id}/scripts/job_staging_ventas.py"
    python_version  = "3"
  }
  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "parquet"
    "--job-language"            = "python"
  }
  number_of_workers = 2
  worker_type       = "G.1X"
  glue_version      = "3.0"
  max_retries       = 1
}

# 4. Glue Job 2: Silver (Spark)
resource "aws_glue_job" "silver_job" {
  name     = "lakehouse-silver-job"
  role_arn = aws_iam_role.glue_service_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.temp.id}/scripts/jobs_silver_ventas.py"
    python_version  = "3"
  }
  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "iceberg"
    "--job-language"            = "python"
  }
  number_of_workers = 2
  worker_type       = "G.1X"
  glue_version      = "3.0"
  max_retries       = 1
}

# 5. Glue Job 3: Gold (Spark)
resource "aws_glue_job" "gold_job" {
  name     = "lakehouse-gold-job"
  role_arn = aws_iam_role.glue_service_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.temp.id}/scripts/jobs_gold_ventas.py"
    python_version  = "3"
  }
  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "iceberg"
    "--job-language"            = "python"
  }
  number_of_workers = 2
  worker_type       = "G.1X"
  glue_version      = "3.0"
  max_retries       = 1
}