package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.server.apis.publicapi.constants.ROOT_PATH

fun removePublicApiPathPrefix(path: String): String {
  return path.removePrefix(ROOT_PATH)
}
