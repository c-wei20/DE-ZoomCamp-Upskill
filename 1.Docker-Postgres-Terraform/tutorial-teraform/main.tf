terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.45.0"
    }
  }
}

provider "google" {
  project     = "my-rides-de-zoomcamp"
  region      = "asia-southeast1"
}

resource "google_storage_bucket" "demo-buncket" {
  name          = "demo-bucket-de-zoomcamp"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}