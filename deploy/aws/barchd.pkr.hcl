# Packer template for the barchd AMI listed on AWS Marketplace.
#
#   packer init deploy/aws
#   packer validate deploy/aws
#   packer build deploy/aws                                 # both AMIs
#   packer build -only='amazon-ebs.amd64' deploy/aws        # x86-64 only
#   packer build -only='amazon-ebs.arm64' deploy/aws        # Graviton only
#
# It starts an Ubuntu 24.04 instance in us-east-1, installs barchd, Debezium
# Server and barch-cdc, hardens SSH, checks the result, cleans the instance and
# saves it as an AMI. One AMI per architecture. See README.md.

packer {
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = "~> 1.3"
    }
  }
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Marketplace takes AMIs from us-east-1 only."
}

variable "instance_type_amd64" {
  type        = string
  default     = "c7i.2xlarge"
  description = "x86-64 build instance. 8 vCPU and 16 GiB build barchd from source in a few minutes."
}

variable "instance_type_arm64" {
  type        = string
  default     = "c7g.2xlarge"
  description = "arm64 (Graviton) build instance."
}

variable "volume_size" {
  type        = number
  default     = 16
  description = "Root volume in GiB. Buyers can launch with a larger one."
}

variable "barch_version" {
  type    = string
  default = "0.5.8b"
}

variable "barch_ref" {
  type    = string
  default = "v0.5.8b"
}

variable "barch_commit" {
  type        = string
  default     = "d915a6ba3e9bb86f7600a2a6028edfb36426b6d8"
  description = "The commit barch_ref must point at. Empty skips the check."
}

variable "barchd_tarball_url" {
  type        = string
  default     = ""
  description = "A prebuilt barchd tarball. Empty builds from source."
}

variable "barchd_tarball_sha256" {
  type    = string
  default = ""
}

variable "debezium_version" {
  type    = string
  default = "3.7.1.Final"
}

variable "debezium_sha256" {
  type        = string
  default     = "388ff55f51228f9f975f5b2eb2bbb95df56f100876f2f9e199fa4f3903a5e9c0"
  description = "sha256 of debezium-server-dist-<version>.tar.gz, as Maven Central publishes it."
}

variable "barchex_url" {
  type        = string
  default     = "https://github.com/tjizep/barchex.git"
  description = "barchex: S3 backup, vector search, watchdog and lent."
}

variable "barchex_commit" {
  type        = string
  default     = "6524a98f2555028204399dce69a5b81d42b9c4ca"
  description = "The barchex commit the image ships. A full hash."
}

locals {
  timestamp = formatdate("YYYYMMDD-hhmm", timestamp())
}

# The settings both architectures share. The build block names the two AMIs and
# sets what differs: instance type, base AMI and AMI name.
source "amazon-ebs" "barchd" {
  region          = var.region
  ami_description = "barch ${var.barch_version} (barchd) on Ubuntu 24.04 LTS"

  ssh_username = "ubuntu"
  # Packer's temporary security group allows SSH from this machine only.
  temporary_security_group_source_public_ip = true

  # Marketplace: HVM, EBS, not encrypted, IMDSv2 only.
  ena_support  = true
  encrypt_boot = false
  imds_support = "v2.0"
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    volume_size           = var.volume_size
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name          = "barchd-${var.barch_version}"
    barch_version = var.barch_version
    barch_ref     = var.barch_ref
    base_ami      = "{{ .SourceAMI }}"
    base_ami_name = "{{ .SourceAMIName }}"
  }
  run_tags = {
    Name = "packer-barchd-build"
  }
}

build {
  source "amazon-ebs.barchd" {
    name          = "amd64"
    instance_type = var.instance_type_amd64
    ami_name      = "barchd-${var.barch_version}-ubuntu-24.04-amd64-${local.timestamp}"
    source_ami_filter {
      filters = {
        name                = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
        root-device-type    = "ebs"
        virtualization-type = "hvm"
        architecture        = "x86_64"
      }
      owners      = ["099720109477"] # Canonical
      most_recent = true
    }
  }

  source "amazon-ebs.barchd" {
    name          = "arm64"
    instance_type = var.instance_type_arm64
    ami_name      = "barchd-${var.barch_version}-ubuntu-24.04-arm64-${local.timestamp}"
    source_ami_filter {
      filters = {
        name                = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"
        root-device-type    = "ebs"
        virtualization-type = "hvm"
        architecture        = "arm64"
      }
      owners      = ["099720109477"] # Canonical
      most_recent = true
    }
  }

  provisioner "shell" {
    inline = ["mkdir -p /tmp/barch-files"]
  }

  provisioner "file" {
    sources = [
      "${path.root}/files/barchd.service",
      "${path.root}/files/barch-firstboot.service",
      "${path.root}/files/barch-firstboot.sh",
      "${path.root}/files/barchd.env",
      "${path.root}/files/00-barch-hardening.conf",
      "${path.root}/files/barch-cdc.py",
      "${path.root}/files/barch-cdc.service",
      "${path.root}/files/cdc.conf",
      "${path.root}/files/debezium.properties",
      "${path.root}/files/debezium.env",
      "${path.root}/files/debezium-server.service",
      "${path.root}/files/debezium-run.sh",
      "${path.root}/files/debezium-queue.luau",
      "${path.root}/debezium-sink",
      "${path.root}/CDC.md",
      "${path.root}/USAGE.md",
    ]
    destination = "/tmp/barch-files/"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    environment_vars = [
      "BARCH_REF=${var.barch_ref}",
      "BARCH_COMMIT=${var.barch_commit}",
      "BARCHD_TARBALL_URL=${var.barchd_tarball_url}",
      "BARCHD_TARBALL_SHA256=${var.barchd_tarball_sha256}",
      "DEBEZIUM_VERSION=${var.debezium_version}",
      "DEBEZIUM_SHA256=${var.debezium_sha256}",
      "BARCHEX_URL=${var.barchex_url}",
      "BARCHEX_COMMIT=${var.barchex_commit}",
    ]
    scripts = [
      "${path.root}/scripts/install-barchd.sh",
      "${path.root}/scripts/install-cdc.sh",
      "${path.root}/scripts/install-barchex.sh",
      "${path.root}/scripts/harden.sh",
      "${path.root}/scripts/check-image.sh",
      # last: it removes the key Packer logs in with
      "${path.root}/scripts/cleanup.sh",
    ]
  }

  post-processor "manifest" {
    output     = "${path.root}/packer-manifest.json"
    strip_path = true
  }
}
