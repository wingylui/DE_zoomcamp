variable "credentials" {
  description = "My Credentials"
  default     = "../keys/consummate-yew-455500-d5-b52aed0e5bc2.json"
}


variable "project" {
  description = "Project"
  default     = "consummate-yew-455500-d5"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "asia-east2"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "asia-east2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "PopulationMigrationWorld"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "de_zoomcamp-population-world"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}