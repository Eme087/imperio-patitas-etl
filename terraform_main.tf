# terraform/main.tf - Infraestructura como código
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Cloud Run Service
resource "google_cloud_run_service" "imperio_patitas_etl" {
  name     = "imperio-patitas-etl"
  location = var.region

  template {
    spec {
      containers {
        image = "gcr.io/${var.project_id}/imperio-patitas-etl:latest"
        
        resources {
          limits = {
            cpu    = "2000m"
            memory = "2Gi"
          }
        }
        
        env {
          name  = "BIGQUERY_PROJECT"
          value = var.project_id
        }
        
        env {
          name  = "BIGQUERY_DATASET"
          value = var.bigquery_dataset
        }
        
        env {
          name = "BSALE_API_TOKEN"
          value_from {
            secret_key_ref {
              name = google_secret_manager_secret.bsale_token.secret_id
              key  = "latest"
            }
          }
        }
      }
      
      container_concurrency = 1
      timeout_seconds      = 3600
    }
    
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "1"
        "run.googleapis.com/cpu-throttling" = "false"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}

# Cloud Scheduler Job - ETL Diario
resource "google_cloud_scheduler_job" "etl_daily" {
  name     = "etl-daily"
  schedule = "0 6 * * *"  # 6:00 AM todos los días
  region   = var.region

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_service.imperio_patitas_etl.status[0].url}/api/v1/scheduler/etl/daily"
    
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

# Cloud Scheduler Job - ETL Incremental (cada 4 horas)
resource "google_cloud_scheduler_job" "etl_incremental" {
  name     = "etl-incremental"
  schedule = "0 */4 * * *"  # Cada 4 horas
  region   = var.region

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_service.imperio_patitas_etl.status[0].url}/api/v1/scheduler/etl/incremental?days=1"
    
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

# Service Account para Scheduler
resource "google_service_account" "scheduler_sa" {
  account_id   = "scheduler-sa"
  display_name = "Cloud Scheduler Service Account"
}

# IAM binding para que Scheduler pueda invocar Cloud Run
resource "google_cloud_run_service_iam_member" "scheduler_invoker" {
  service  = google_cloud_run_service.imperio_patitas_etl.name
  location = google_cloud_run_service.imperio_patitas_etl.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# Secret Manager para BSALE_API_TOKEN
resource "google_secret_manager_secret" "bsale_token" {
  secret_id = "bsale-api-token"
  
  replication {
    automatic = true
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "imperio_patitas" {
  dataset_id    = var.bigquery_dataset
  friendly_name = "Imperio Patitas ETL Data"
  description   = "Dataset para datos de Bsale sincronizados"
  location      = "US"
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "bigquery_dataset" {
  description = "BigQuery Dataset Name"
  type        = string
  default     = "imperio_patitas"
}

# Outputs
output "cloud_run_url" {
  description = "URL del servicio Cloud Run"
  value       = google_cloud_run_service.imperio_patitas_etl.status[0].url
}

output "bigquery_dataset_id" {
  description = "ID del dataset BigQuery"
  value       = google_bigquery_dataset.imperio_patitas.dataset_id
}
