package io.airbyte.commons.audit

data class User(val userId: String, val email: String? = null, val ipAddress: String? = null, val userAgent: String? = null)
