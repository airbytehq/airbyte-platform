/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("airbyte.data-plane-credentials")
data class DataplaneCredentials(
  val clientId: String,
  val clientSecret: String,
)
