
resource "random_string" "unique_id" {
  length  = 8
  upper   = false
  lower   = true
  numeric = false
  special = false
}

module "s3" {
  source = "git::https://github.com/terraform-yc-modules/terraform-yc-s3.git?ref=1.0.1"

  bucket_name = "mlops-homework-${random_string.unique_id.result}"

  acl = "public-read"
  force_destroy = true
}
