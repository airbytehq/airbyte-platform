/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.helpers

import io.airbyte.api.server.constants.AIRBYTE_API_AUTH_HEADER_VALUE

/**
 * Not used for OSS, in OSS this will return null.
 */
fun getLocalUserInfoIfNull(userInfo: String?): String? {
  return userInfo ?: System.getenv(AIRBYTE_API_AUTH_HEADER_VALUE)
}
