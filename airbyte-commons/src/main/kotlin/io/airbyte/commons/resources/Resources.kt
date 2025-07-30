/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.resources

import java.io.File
import java.net.URLDecoder
import java.util.jar.JarFile

object Resources {
  /**
   * Returns a list of all files and directories for the specified [dir].
   *
   * @return list of file/directory names if found, an empty list otherwise.
   */
  fun list(dir: String): List<String> {
    val loader = Thread.currentThread().contextClassLoader
    val resource = loader.getResource(dir) ?: return emptyList()

    return when (resource.protocol) {
      "file" -> File(resource.toURI()).list()?.toList() ?: emptyList()
      "jar" -> {
        val jarPath = resource.path.substringBefore("!").removePrefix("file:")
        val normalizedPath = "${dir.removeSuffix("/")}/"
        JarFile(URLDecoder.decode(jarPath, "UTF-8"))
          .entries()
          .asSequence()
          .map { it.name }
          .filter { it.startsWith(normalizedPath) }
          .mapNotNull { entry ->
            entry
              .removePrefix(normalizedPath)
              .takeWhile { it != '/' }
              .takeIf { it.isNotEmpty() }
          }.distinct()
          .toList()
      }
      else -> emptyList()
    }
  }

  /**
   * Reads the [resource], returning it as a string.
   *
   * @throws [IllegalArgumentException] if [resource] does not exist
   * @return the contents of [resource]
   */
  fun read(resource: String): String =
    this::class.java.getResource("/$resource")?.readText() ?: throw IllegalArgumentException("Resource not found: $resource")

  /**
   * Attempts to read the [resource].
   *
   * @return the contents of [resource] if successful, null otherwise
   */
  fun readOrNull(resource: String): String? = this::class.java.getResource("/$resource")?.readText()

  /**
   * Access a java Resource as a file.
   *
   * @param resource name of resource
   * @return file handle to the Resource
   * @throws [IllegalArgumentException] if [resource] does not exist
   */
  fun readResourceAsFile(resource: String): File =
    this::class.java.getResource("/$resource")?.let { File(it.toURI()) }
      ?: throw IllegalArgumentException("Resource not found: $resource")
}
