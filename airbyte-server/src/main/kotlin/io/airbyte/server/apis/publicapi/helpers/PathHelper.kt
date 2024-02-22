package io.airbyte.server.apis.publicapi.helpers

fun removePublicApiPathPrefix(path: String): String {
  return path.removePrefix("/public/api")
}
