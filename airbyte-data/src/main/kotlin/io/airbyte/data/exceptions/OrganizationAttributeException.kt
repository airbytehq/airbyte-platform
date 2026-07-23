/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.exceptions

/**
 * Thrown when organization customer attributes cannot be resolved from the GCS bucket (missing
 * credentials, no source file, or unreadable/unparseable content). These are server-side faults, so
 * left uncaught this surfaces as a 500 via the platform's UncaughtExceptionHandler.
 */
class OrganizationAttributeException(
  message: String,
  cause: Throwable? = null,
) : RuntimeException(message, cause)
