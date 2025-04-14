terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}


resource "google_bigquery_table" "fact_population" {
  dataset_id = var.bq_dataset_name
  table_id   = "fact_population"
  clustering = ["Code"]
  
  schema = jsonencode([
    {
      name = "Code"
      type = "STRING"
    },
    {
      name = "Year"
      type = "INTEGER"
    },
    {
      name = "birth_rate"
      type = "FLOAT"
    },
    {
      name = "life_expectancy"
      type = "FLOAT"
    },
    {
      name = "international_immigrants"
      type = "FLOAT"
    },
    {
      name = "refugees"
      type = "FLOAT"
    }
  ])

  range_partitioning {
    field = "Year"

    range {
      start    = 1950
      end      = 2100
      interval = 1
    }
  }

}

resource "google_bigquery_table" "dim_country" {
  dataset_id = var.bq_dataset_name
  table_id   = "dim_country"
  
  schema = jsonencode([
    {
      name = "Code"
      type = "STRING"
    },
    {
      name = "Entity"
      type = "STRING"
    }
  ])
}