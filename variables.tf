variable "credentials" {
  description = "My Credentials"
  type        = string
}

variable "project" {
  description = "My Main Project"
  type        = string
}

variable "region" {
  description = "Project Region"
  type        = string
}

variable "location" {
  description = "Project Location"
  type        = string
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  type        = string
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
}

variable "bq_temp_bucket_name" {
  description = "My Temporary GCS Bucket Name"
  type        = string
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
}


