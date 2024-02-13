package helpers

fun removePublicApiPathPrefix(path: String): String {
  return path.removePrefix("/public/api")
}
