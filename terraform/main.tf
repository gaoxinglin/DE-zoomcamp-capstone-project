terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project           = var.project_id
  region            = var.region
  billing_project   = var.project_id
  user_project_override = true
}

# ── GCS Bucket ──────────────────────────────────────────────────────────────

resource "google_storage_bucket" "nz_electricity" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 365 }
    action    { type = "Delete" }
  }
}

# ── BigQuery Datasets ────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.region
  description = "Raw layer: external tables pointing to GCS CSVs"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  location    = var.region
  description = "Staging layer: unpivoted and standardised generation data"
}

resource "google_bigquery_dataset" "core" {
  dataset_id  = "core"
  location    = var.region
  description = "Core layer: dim_plant and fct_generation"
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts"
  location    = var.region
  description = "Mart layer: aggregated tables for Looker Studio"
}

# ── Billing Budget Alert ─────────────────────────────────────────────────────

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_billing_budget" "alert" {
  billing_account = var.billing_account
  display_name    = "nz-electricity-budget"

  amount {
    specified_amount {
      currency_code = "NZD"
      units         = "10"
    }
  }

  budget_filter {
    projects = ["projects/${data.google_project.project.number}"]
  }

  threshold_rules { threshold_percent = 0.5 }
  threshold_rules { threshold_percent = 0.9 }
  threshold_rules { threshold_percent = 1.0 }
}
