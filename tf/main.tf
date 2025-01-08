terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.12.0"
    }
  }
}
provider "confluent" {
  cloud_api_key         = var.confluent_cloud_api_key
  cloud_api_secret      = var.confluent_cloud_api_secret
}

data "confluent_organization" "main" {}

data "confluent_environment" "ksilin" {
  id = var.environment_id
}

data "confluent_kafka_cluster" "std_test" {
    id             = "lkc-6w3rv2"
  environment {
    id = data.confluent_environment.ksilin.id
  }
}

locals {
  cloud  = "AWS"
  region = "eu-central-1"
}

data "confluent_flink_region" "eu-central-1" {
  cloud  = local.cloud
  region = local.region
}

data "confluent_flink_compute_pool" "aws_pool" {
  id = "lfcp-kknvdm"
  environment {
    id = data.confluent_environment.ksilin.id
  }
}

resource "confluent_flink_statement" "random_int_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.confluent_environment.ksilin.id
  }
  compute_pool {
    id = data.confluent_flink_compute_pool.aws_pool.id
  }
  principal {
    id = var.flink_principal_id
  }
  statement  = "CREATE TABLE random_int_table(ts TIMESTAMP_LTZ(3), random_value INT);"
  properties = {
    "sql.current-catalog"  = data.confluent_environment.ksilin.display_name
    "sql.current-database" = data.confluent_kafka_cluster.std_test.display_name
  }
  rest_endpoint = data.confluent_flink_region.eu-central-1.rest_endpoint
  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  lifecycle {
    prevent_destroy = true
  }
}

// TF 2.12 is broken for artifact upload
/* confluent_flink_artifact.tshirt_sizing: Creating...
╷
│ Error: error uploading Flink Artifact: error fetching presigned upload URL 400 Bad Request: environment Query Path Param is required
│
│   with confluent_flink_artifact.tshirt_sizing,
│   on main.tf line 51, in resource "confluent_flink_artifact" "tshirt_sizing":
│   51: resource "confluent_flink_artifact" "tshirt_sizing" {
│
╵ */
/* resource "confluent_flink_artifact" "tshirt_sizing" {
  environment {
    id = var.environment_id
  }
  class          = "org.example.TshitSizingSmaller"
  region         = "eu-central-1"
  cloud          = "AWS"
  display_name   = "flink-udf-tshirtsize"
  content_format = "JAR"
  artifact_file  = var.artifact_file
}*/

#locals {
#  plugin_id  = confluent_flink_artifact.main.id
#  version_id = confluent_flink_artifact.main.versions[0].version
#}

#resource "confluent_flink_statement" "create-function" {
#  # Can skip the version_id part of the statement as it is now optional
#  statement = "CREATE FUNCTION is_smaller  AS 'io.confluent.flink.table.modules.remoteudf.TShirtSizingIsSmaller' USING JAR 'confluent-artifact://${local.plugin_id}/${local.version_id}';"
#  properties = {
#    "sql.current-catalog"  = var.current_catalog
#    "sql.current-database" = var.current_database
#  }
#}
