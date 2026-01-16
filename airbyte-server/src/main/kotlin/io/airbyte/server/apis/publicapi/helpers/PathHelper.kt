/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.ROOT_PATH

fun removePublicApiPathPrefix(path: String): String = path.removePrefix(API_PATH + ROOT_PATH).removePrefix(ROOT_PATH)
