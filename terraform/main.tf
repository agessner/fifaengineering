terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file("../googlecredentials.json")
  project = "fifaeng"
  region = "us-central1"
}

resource "google_composer_environment" "fifaeng" {
  name   = "fifaeng"
  region = "us-central1"
  config {
    node_count = 4

    node_config {
      zone = "us-central1-f"
      machine_type = "n1-standard-2"
      disk_size_gb = 20
      service_account = "dataloader@fifaeng.iam.gserviceaccount.com"
    }

    software_config {
      image_version = "composer-latest-airflow-1.10.6"
      python_version = 3
      env_variables = {
        SCRAPY_PATH = "/home/airflow/gcs/dags/dependencies/scrapy_sofifa/"
      }
      airflow_config_overrides = {
        core-worker_concurrency = 10
      }
      pypi_packages = {
        scrapy = "==2.1.0"
        attrs = "==19.2.0"
      }
    }
  }
}
