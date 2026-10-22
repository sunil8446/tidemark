# Dev environment: VPC + S3 lakehouse + least-privilege roles + one k3s node.
# EKS is intentionally not here; it is applied separately and only during
# benchmark windows (see docs/adr and the plan's cost notes).
#
#   terraform init && terraform plan -var admin_cidr=$(curl -s ifconfig.me)/32 \
#     -var ssh_public_key="$(cat ~/.ssh/id_ed25519.pub)" -var budget_email=you@example.com

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
  # Remote state (S3 + lockfile) belongs here before anyone else applies:
  # backend "s3" { bucket = "...-tfstate" key = "dev/terraform.tfstate" region = "us-east-1" use_lockfile = true }
}

variable "region" {
  type    = string
  default = "us-east-1"
}
variable "prefix" {
  type        = string
  default     = "tidemark-dev"
  description = "Globally unique bucket prefix; change it if taken."
}
variable "admin_cidr" { type = string }
variable "ssh_public_key" { type = string }
variable "budget_email" {
  type        = string
  default     = ""
  description = "Empty = no budget resource (set the budget by hand, see docs/aws-setup.md)."
}
variable "monthly_budget_usd" {
  type    = number
  default = 30
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "tidemark", env = "dev", managed_by = "terraform" }
  }
}

data "aws_availability_zones" "available" { state = "available" }

module "network" {
  source = "../../modules/network"
  name   = var.prefix
  azs    = slice(data.aws_availability_zones.available.names, 0, 2)
}

module "lake" {
  source = "../../modules/lake"
  prefix = var.prefix
}

module "roles" {
  source                 = "../../modules/irsa"
  name                   = var.prefix
  lake_bucket_arn        = module.lake.lake_bucket_arn
  checkpoints_bucket_arn = module.lake.checkpoints_bucket_arn
}

module "k3s" {
  source         = "../../modules/k3s"
  name           = var.prefix
  vpc_id         = module.network.vpc_id
  subnet_id      = module.network.public_subnet_ids[0]
  admin_cidr     = var.admin_cidr
  ssh_public_key = var.ssh_public_key
  role_names     = [module.roles.role_names["flink"]]
}

resource "aws_budgets_budget" "monthly" {
  count        = var.budget_email == "" ? 0 : 1
  name         = "${var.prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_email]
    }
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
}

output "k3s_public_ip" { value = module.k3s.public_ip }
output "lake_bucket" { value = module.lake.lake_bucket }
output "checkpoints_bucket" { value = module.lake.checkpoints_bucket }
output "role_arns" { value = module.roles.role_arns }
