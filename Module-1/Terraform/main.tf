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






# resource "google_compute_instance" "de-test-vm" {
#   boot_disk {
#     auto_delete = true
#     device_name = "de-test-vm"

#     initialize_params {
#       image = "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20240126"
#       size  = 30
#       type  = "pd-balanced"
#     }

#     mode = "READ_WRITE"
#   }

#   can_ip_forward      = false
#   deletion_protection = false
#   enable_display      = false

#   labels = {
#     goog-ec-src = "vm_add-tf"
#   }

#   machine_type     = "n1-standard-2"
#   min_cpu_platform = "Intel Skylake"
#   name             = "de-test-vm"

#   network_interface {
#     access_config {
#       network_tier = "PREMIUM"
#     }

#     queue_count = 0
#     stack_type  = "IPV4_ONLY"
#     subnetwork  = "projects/learning-terraform-412615/regions/us-west1/subnetworks/default"
#   }

#   scheduling {
#     automatic_restart   = true
#     on_host_maintenance = "MIGRATE"
#     preemptible         = false
#     provisioning_model  = "STANDARD"
#   }

#   service_account {
#     email  = "terraform-admin@learning-terraform-412615.iam.gserviceaccount.com"
#     scopes = ["https://www.googleapis.com/auth/cloud-platform"]
#   }

#   shielded_instance_config {
#     enable_integrity_monitoring = true
#     enable_secure_boot          = false
#     enable_vtpm                 = true
#   }

#   zone = "us-west1-b"
# }
