variable "prefix" {
  description = "Resource name prefix; keeps all provisioned resources greppable."
  type        = string
  default     = "github-events"
}

variable "environment" {
  description = "Deployment environment tag (dev/stg/prod)."
  type        = string
  default     = "dev"
}

variable "storage_root" {
  description = "Cloud storage root (external location URL) backing the catalogs."
  type        = string
}

variable "data_engineers_group" {
  description = "Account-level group granted read/write on every layer."
  type        = string
  default     = "data-engineers"
}

variable "analysts_group" {
  description = "Account-level group granted read-only access to gold."
  type        = string
  default     = "analysts"
}

variable "svc_ingest_principal" {
  description = "Service principal application ID running the bronze copy-into load."
  type        = string
}

variable "svc_vault_principal" {
  description = "Service principal application ID running vault loads and gold builds."
  type        = string
}
