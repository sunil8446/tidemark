# Lakehouse and checkpoint buckets: private, encrypted, TLS-only.

variable "prefix" { type = string }
variable "checkpoint_retention_days" {
  type    = number
  default = 7
}

resource "aws_s3_bucket" "lake" {
  bucket = "${var.prefix}-lake"
}

resource "aws_s3_bucket" "checkpoints" {
  bucket        = "${var.prefix}-checkpoints"
  force_destroy = true # checkpoints are disposable; the lake is not
}

locals {
  buckets = { lake = aws_s3_bucket.lake, checkpoints = aws_s3_bucket.checkpoints }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.buckets
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = local.buckets
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.buckets
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json
}

resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"
    filter {}
    expiration { days = var.checkpoint_retention_days }
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}

output "lake_bucket" { value = aws_s3_bucket.lake.id }
output "lake_bucket_arn" { value = aws_s3_bucket.lake.arn }
output "checkpoints_bucket" { value = aws_s3_bucket.checkpoints.id }
output "checkpoints_bucket_arn" { value = aws_s3_bucket.checkpoints.arn }
