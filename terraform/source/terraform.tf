terraform {
  required_version = ">= 1.1.9"
  required_providers {
    google      = ">= 4.21"
    google-beta = ">= 4.21"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}
