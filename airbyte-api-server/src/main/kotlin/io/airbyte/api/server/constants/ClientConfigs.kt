/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.constants

/*
 * Reference the environment variable, instead of the resolved property, so
 * that usage by any generated Micronaut client doesn't need to strip off
 * the root path added to the resolved property in the application.yml file.
 */
const val INTERNAL_API_HOST = "\${INTERNAL_API_HOST}"
const val AUTH_HEADER = "Authorization"
const val ENDPOINT_API_USER_INFO_HEADER = "X-Endpoint-API-UserInfo"
const val ANALYTICS_HEADER = "X-Airbyte-Analytic-Source"
const val ANALYTICS_HEADER_VALUE = "airbyte-api"

const val AIRBYTE_API_AUTH_HEADER_VALUE = "AIRBYTE_API_AUTH_HEADER_VALUE"
const val API_DOC_URL = "https://reference.airbyte.com"
