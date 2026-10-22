# Least-privilege S3 access, one role per workload.
#
# On EKS the role is assumed through IRSA (the pod's service-account token);
# on the k3s node it is attached to the instance profile instead. Each
# workload gets only the actions and prefixes it needs:
#   flink  : read/write lake/warehouse/*, read/write checkpoints/*
#   trino  : read/write lake/warehouse/* (dbt writes marts; maintenance rewrites files)
#   ingest : no S3 at all (Kafka only), so it has no role here

variable "name" { type = string }
variable "lake_bucket_arn" { type = string }
variable "checkpoints_bucket_arn" { type = string }
variable "oidc_provider_arn" {
  type        = string
  default     = ""
  description = "EKS OIDC provider ARN. Empty = trust EC2 instead (k3s node)."
}
variable "oidc_provider_url" {
  type    = string
  default = ""
}
variable "namespace" {
  type    = string
  default = "tidemark"
}

locals {
  workloads = {
    flink = {
      service_account = "flink"
      rw = [
        "${var.lake_bucket_arn}/warehouse/*",
        "${var.checkpoints_bucket_arn}/*",
      ]
      buckets = [var.lake_bucket_arn, var.checkpoints_bucket_arn]
    }
    trino = {
      service_account = "trino"
      rw              = ["${var.lake_bucket_arn}/warehouse/*"]
      buckets         = [var.lake_bucket_arn]
    }
  }
}

data "aws_iam_policy_document" "trust" {
  for_each = local.workloads
  statement {
    actions = var.oidc_provider_arn == "" ? ["sts:AssumeRole"] : ["sts:AssumeRoleWithWebIdentity"]
    dynamic "principals" {
      for_each = var.oidc_provider_arn == "" ? [1] : []
      content {
        type        = "Service"
        identifiers = ["ec2.amazonaws.com"]
      }
    }
    dynamic "principals" {
      for_each = var.oidc_provider_arn == "" ? [] : [1]
      content {
        type        = "Federated"
        identifiers = [var.oidc_provider_arn]
      }
    }
    dynamic "condition" {
      for_each = var.oidc_provider_arn == "" ? [] : [1]
      content {
        test     = "StringEquals"
        variable = "${trimprefix(var.oidc_provider_url, "https://")}:sub"
        values   = ["system:serviceaccount:${var.namespace}:${each.value.service_account}"]
      }
    }
  }
}

data "aws_iam_policy_document" "access" {
  for_each = local.workloads
  statement {
    sid       = "ListOwnBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = each.value.buckets
  }
  statement {
    sid       = "ReadWriteOwnPrefixes"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = each.value.rw
  }
}

resource "aws_iam_role" "this" {
  for_each           = local.workloads
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.trust[each.key].json
}

resource "aws_iam_role_policy" "this" {
  for_each = local.workloads
  name     = "s3-least-privilege"
  role     = aws_iam_role.this[each.key].id
  policy   = data.aws_iam_policy_document.access[each.key].json
}

output "role_arns" { value = { for k, r in aws_iam_role.this : k => r.arn } }
output "role_names" { value = { for k, r in aws_iam_role.this : k => r.name } }
