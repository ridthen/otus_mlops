terraform {
  required_version = ">= 1.3.0"

  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
      version = "> 0.92"
    }
  
    local = {
      source  = "hashicorp/local"
      version = "2.2.3"
    }
    random = {
      source  = "hashicorp/random"
      version = "> 3.5.1"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "> 5.1"
    }
  }
  backend "s3" {
    endpoint = "storage.yandexcloud.net"
    region = "ru-central1"

    bucket = "bucket name for terraform state file"
    key = "tf.state"

    skip_region_validation = true
    skip_credentials_validation = true

    dynamodb_endpoint = "dynamodb url for terraform lock"
    dynamodb_table = "table for lock"
  }
}

provider "yandex" {
  cloud_id = "cloud identifier"
  folder_id = "folder identifier"
}

provider "aws" {
  skip_region_validation      = true
  skip_credentials_validation = true
  skip_requesting_account_id  = true
}

provider "local" {}

provider "random" {}
