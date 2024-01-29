terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  project     = var.project
  credentials = file(var.credentials)
  region      = var.region
}


resource "google_storage_bucket" "gcs-bucket" {
  name          = var.gcs_bucket_name
  storage_class = var.gcs_storage_class
  location      = var.location
}



resource "google_bigquery_dataset" "bq-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
