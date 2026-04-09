/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants

/**
 * Shared constants for Kubernetes NetworkPolicy labeling and matching.
 * Used by both the workload launcher (pod labeling) and infra-worker (policy creation)
 * to ensure the hash algorithm, label keys, and namespace stay in sync.
 */
object NetworkPolicyConstants {
  const val TOKEN_HASH_LABEL_KEY = "airbyte/networkSecurityTokenHash"
  const val WORKSPACE_ID_LABEL_KEY = "airbyte/workspaceId"
  const val TOKEN_HASH_SALT = "airbyte.network.security.token"
  const val TOKEN_HASH_TRUNCATION_LENGTH = 50
  const val NETWORK_POLICY_NAMESPACE = "jobs"
}
