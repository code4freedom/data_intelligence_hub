// Terraform skeleton for GCP resources used by RVTools Chunker
// Provider and resource modules should be filled with real values before applying.

terraform {
  required_version = ">= 1.0"
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "raw" {
  name     = "${var.project}-rvtools-raw"
  location = var.region
}

resource "google_bigquery_dataset" "rvtools" {
  dataset_id = "rvtools"
  location   = var.region
}

// Add Cloud Run, Dataflow templates, Pub/Sub, IAM bindings as needed
