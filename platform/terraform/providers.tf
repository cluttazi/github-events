# Workspace-scoped provider: this module owns the Unity Catalog layout
# (catalogs, schemas, grants) inside an existing workspace + metastore.
#
# Authentication is intentionally NOT hard-coded: use environment variables
# (DATABRICKS_HOST plus DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET for
# OAuth M2M, or DATABRICKS_TOKEN). Nothing in this module is ever applied
# from the local demo environment — see README.md.

provider "databricks" {}
