resource "yandex_vpc_security_group" "sg-internet" {
  description = "Allow any outgoing traffic to the Internet"
  name        = "sg-internet"
  network_id  = yandex_vpc_network.this.id

  egress {
    description    = "Allow any outgoing traffic to the Internet"
    protocol       = "ANY"
    from_port      = 0
    to_port        = 65535
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "yandex_vpc_security_group" "sg-data-proc-cluster" {
  description = "Security group for the Yandex Data Proc cluster"
  name        = "sg-data-proc-cluster"
  network_id  = yandex_vpc_network.this.id

  ingress {
    description       = "Allow any traffic within one security group"
    protocol          = "ANY"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
}

resource "yandex_vpc_security_group" "sg-nat-instance" {
  description = "Security group for the NAT instance"
  name        = "sg-nat-instance"
  network_id  = yandex_vpc_network.this.id

  ingress {
    description    = "Allow SSH connections to the NAT instance"
    protocol       = "TCP"
    port           = 22
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description       = "Allow connections from the Data Proc cluster"
    protocol          = "ANY"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
}


resource "yandex_dataproc_cluster" "this" {
  depends_on = [yandex_resourcemanager_folder_iam_binding.dataproc]

  bucket      = yandex_storage_bucket.this.bucket
  description = "Dataproc Cluster created by Terraform"
  name        = "dataproc-cluster"
  labels = {
    created_by = "terraform"
  }
  service_account_id = yandex_iam_service_account.dataproc.id
  zone_id            = "ru-central1-a"

  security_group_ids = [
    yandex_vpc_security_group.sg-internet.id,         # Allow any outgoing traffic to the Internet.
    yandex_vpc_security_group.sg-data-proc-cluster.id, # Allow connections from VM and inside the security group.
    yandex_vpc_security_group.sg-nat-instance.id
  ]

  cluster_config {
    # Certain cluster version can be set, but better to use default value (last stable version)
    # version_id = "2.0"

    hadoop {
      services = ["HDFS", "YARN", "SPARK", "TEZ", "MAPREDUCE", "HIVE"]
      properties = {
        "yarn:yarn.resourcemanager.am.max-attempts" = 5
      }
      ssh_public_keys = [
        "ssh-ed25519 public key string"]

    }

    subcluster_spec {
      name = "main"
      role = "MASTERNODE"
      resources {
        resource_preset_id = "s3-c2-m8"
        disk_type_id       = "network-hdd"
        disk_size          = 40
      }
      subnet_id   = yandex_vpc_subnet.this.id
      hosts_count = 1
      assign_public_ip = true

    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = "s3-c4-m16"
        disk_type_id       = "network-hdd"
        disk_size          = 128
      }
      subnet_id   = yandex_vpc_subnet.this.id
      hosts_count = 3
    }
    
  }
}

resource "yandex_vpc_network" "this" {}

resource "yandex_vpc_subnet" "this" {
  zone           = "ru-central1-a"
  network_id     = yandex_vpc_network.this.id
  v4_cidr_blocks = ["10.1.0.0/24"]
  route_table_id = yandex_vpc_route_table.rt.id
}

resource "yandex_vpc_gateway" "nat_gateway" {
  folder_id      = "folder id"
  name = "dataproc-gateway"
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "rt" {
  folder_id      = "folder id"
  name       = "dataproc-route-table"
  network_id = yandex_vpc_network.this.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_iam_service_account" "dataproc" {
  name        = "dataproc"
  description = "service account to manage Dataproc Cluster"
}

data "yandex_resourcemanager_folder" "this" {
  folder_id = "folder id"
}

resource "yandex_resourcemanager_folder_iam_binding" "dataproc" {
  folder_id = data.yandex_resourcemanager_folder.this.id
  role      = "mdb.dataproc.agent"
  members = [
    "serviceAccount:${yandex_iam_service_account.dataproc.id}",
  ]
}

// required in order to create bucket
resource "yandex_resourcemanager_folder_iam_binding" "bucket-creator" {
  folder_id = data.yandex_resourcemanager_folder.this.id
  role      = "storage.editor"
  members = [
    "serviceAccount:${yandex_iam_service_account.dataproc.id}",
  ]
}

resource "yandex_iam_service_account_static_access_key" "this" {
  service_account_id = yandex_iam_service_account.dataproc.id
}

resource "yandex_kms_symmetric_key" "key-a" {
  name              = "dataproc-bucket-key"
  description       = "dataproc-bucket sym key"
  default_algorithm = "AES_128"
  rotation_period   = "8760h"
}

resource "yandex_storage_bucket" "this" {
  depends_on = [
    yandex_resourcemanager_folder_iam_binding.bucket-creator
  ]

  bucket     = "dataproc-keys-bucket"
  access_key = yandex_iam_service_account_static_access_key.this.access_key
  secret_key = yandex_iam_service_account_static_access_key.this.secret_key

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = yandex_kms_symmetric_key.key-a.id
        sse_algorithm     = "aws:kms"
      }
    }
  }
}
