/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

// Shared by the workload launcher (DirectCheckRouter, which decides whether to offer a check to the direct job runner)
// and the direct job runner (DirectCheckAllowlist, which decides whether to take it). Both read [DirectCheckImages] and
// must agree on what it lists and which images match, and neither application module can depend on the other, so the
// helpers live here next to the flag.

/**
 * The connector names in a [DirectCheckImages] value (`source-postgres, source-mysql`). Whitespace and empty entries
 * are ignored.
 */
fun parseDirectCheckConnectors(value: String): Set<String> =
  value
    .split(",")
    .map { it.trim() }
    .filter { it.isNotEmpty() }
    .toSet()

/**
 * Whether [image] is `airbyte/<connector>:<version>` for one of [connectors]. Only Airbyte's own images match: the
 * direct job runner runs the jar published for that connector and version, which a custom image with the same name
 * isn't built from. An image with a registry prefix, a digest or no tag doesn't match, and its check runs in a pod.
 */
fun isListedAirbyteConnector(
  image: String,
  connectors: Set<String>,
): Boolean {
  val repository = image.substringBeforeLast(':', missingDelimiterValue = "")
  val connector = repository.removePrefix("airbyte/")
  return repository == "airbyte/$connector" && connector in connectors && image.substringAfterLast(':').isNotEmpty()
}
