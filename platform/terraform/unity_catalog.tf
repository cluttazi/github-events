# Unity Catalog: the medallion catalog/schema layout with grants-as-code.
#
# The privilege matrix lives in governance/unity_catalog/grants.yaml and is
# mirrored here; keep the two in sync (the governance module renders the YAML
# into the human-readable access matrix used for audit evidence).
#
# Layer mapping (medallion <-> Data Vault 2.0):
#   bronze  — landing / persistent staging (github events + quarantine + ops)
#   silver  — raw_vault (hubs/links/satellites) + business_vault (PIT/bridge)
#   gold    — information marts + observability

locals {
  catalogs = {
    bronze = "Raw immutable event ingestion. Append-only, quarantine included."
    silver = "Data Vault 2.0: raw vault (insert-only) and business vault (derived)."
    gold   = "Information marts consumed by analytics."
  }

  schemas = {
    bronze = ["github", "quarantine", "ops"]
    silver = ["raw_vault", "business_vault"]
    gold   = ["marts", "observability"]
  }

  catalog_schemas = merge([
    for catalog, schema_names in local.schemas : {
      for schema in schema_names : "${catalog}.${schema}" => {
        catalog = catalog
        schema  = schema
      }
    }
  ]...)
}

resource "databricks_catalog" "layer" {
  for_each = local.catalogs

  name         = "${var.prefix}_${var.environment}_${each.key}"
  comment      = each.value
  storage_root = "${var.storage_root}/${each.key}"
  properties = {
    project     = var.prefix
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "databricks_schema" "layer" {
  for_each = local.catalog_schemas

  catalog_name = databricks_catalog.layer[each.value.catalog].name
  name         = each.value.schema
  comment      = "Managed by terraform; see governance/unity_catalog/grants.yaml"
}

# --- grants: mirror of governance/unity_catalog/grants.yaml ---------------

resource "databricks_grants" "bronze" {
  catalog = databricks_catalog.layer["bronze"].name

  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = var.svc_ingest_principal
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = var.svc_vault_principal
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "silver" {
  catalog = databricks_catalog.layer["silver"].name

  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = var.svc_vault_principal
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}

resource "databricks_grants" "gold" {
  catalog = databricks_catalog.layer["gold"].name

  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = var.svc_vault_principal
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}
