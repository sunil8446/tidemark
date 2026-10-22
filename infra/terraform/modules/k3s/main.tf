# One EC2 instance running k3s: the cheap always-on target (plan cut line).
# Spot by default; the admin CIDR is the only inbound source.

variable "name" { type = string }
variable "vpc_id" { type = string }
variable "subnet_id" { type = string }
variable "admin_cidr" {
  type        = string
  description = "Your IP/32. SSH and the Kubernetes API are only open to this."
}
variable "instance_type" {
  type    = string
  default = "t4g.xlarge" # 4 vCPU / 16 GB arm64
}
variable "spot" {
  type    = bool
  default = true
}
variable "disk_gb" {
  type    = number
  default = 50
}
variable "ssh_public_key" { type = string }
variable "role_names" {
  type        = list(string)
  description = "IAM roles whose S3 policies this node needs (from the irsa module, trust = EC2)."
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }
}

resource "aws_security_group" "node" {
  name   = "${var.name}-k3s"
  vpc_id = var.vpc_id
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "Kubernetes API"
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "admin" {
  key_name   = "${var.name}-admin"
  public_key = var.ssh_public_key
}

resource "aws_iam_instance_profile" "node" {
  name = "${var.name}-k3s"
  role = var.role_names[0]
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = var.role_names[0]
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_instance" "node" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.node.id]
  key_name               = aws_key_pair.admin.key_name
  iam_instance_profile   = aws_iam_instance_profile.node.name

  dynamic "instance_market_options" {
    for_each = var.spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        instance_interruption_behavior = "stop"
        spot_instance_type             = "persistent"
      }
    }
  }

  root_block_device {
    volume_size = var.disk_gb
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 2          # pods need one extra hop to reach IMDS
  }

  user_data = <<-EOT
    #!/bin/bash
    set -euo pipefail
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable traefik --write-kubeconfig-mode 0640 --tls-san $(curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/public-ipv4)" sh -
  EOT

  tags = { Name = "${var.name}-k3s" }
}

output "public_ip" { value = aws_instance.node.public_ip }
output "instance_id" { value = aws_instance.node.id }
