variable "credentials" {
  description = "My gcp credentials"
  default     = "/home/shakik/Documents/git-repos/data-engineering-course/Module-1/Terraform/keys/key.json"
}

variable "project" {
  description = "Project Name"
  default     = "learning-terraform-412615"
}

variable "region" {
  description = "Project region"
  default     = "us-west1"
}


variable "location" {
  description = "Project location"
  default     = "US"
}


variable "bq_dataset_name" {
  description = "My bq dataset name"
  default     = "bq_demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "learning-terraform-412615"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}
