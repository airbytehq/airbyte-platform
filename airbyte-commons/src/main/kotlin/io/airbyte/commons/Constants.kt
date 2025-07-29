/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons

import java.util.UUID

const val AUTO_DATAPLANE_GROUP = "AUTO"
const val US_DATAPLANE_GROUP = "US"
const val EU_DATAPLANE_GROUP = "EU"

/**
 * Each installation of Airbyte comes with a default organization. The ID of this organization is
 * hardcoded to the 0 UUID so that it can be consistently retrieved.
 */
@JvmField
val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

/**
 * Each installation of Airbyte comes with a default user. The ID of this user is hardcoded to the 0
 * UUID so that it can be consistently retrieved.
 */
@JvmField
val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
