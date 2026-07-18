output "catalog_names" {
  description = "Provisioned catalog names by medallion layer."
  value       = { for layer, catalog in databricks_catalog.layer : layer => catalog.name }
}

output "schema_full_names" {
  description = "Provisioned schema full names (catalog.schema)."
  value       = [for schema in databricks_schema.layer : "${schema.catalog_name}.${schema.name}"]
}
