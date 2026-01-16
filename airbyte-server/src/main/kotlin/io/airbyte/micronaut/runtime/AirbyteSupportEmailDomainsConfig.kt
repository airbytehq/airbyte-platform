/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.annotation.ConfigurationProperties

internal const val DEFAULT_SUPPORT_CLOUD_EMAIL_DOMAIN = "airbyte.io"

@ConfigurationProperties("airbyte.support-email-domains")
data class AirbyteSupportEmailDomainsConfig(
  val oss: String = "",
  val cloud: String = DEFAULT_SUPPORT_CLOUD_EMAIL_DOMAIN,
)
