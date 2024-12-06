package io.airbyte.audit.logging

data class User(val userId: String, val email: String? = null, var ipAddress: String? = null, var userAgent: String? = null)
