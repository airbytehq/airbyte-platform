/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.contributions

import io.airbyte.commons.server.builder.exceptions.CircularReferenceException
import io.airbyte.commons.server.builder.exceptions.ManifestParserException
import io.airbyte.commons.server.builder.exceptions.UndefinedReferenceException
import org.yaml.snakeyaml.Yaml

const val REF_TAG = "\$ref"
const val REF_VALUE = "#/"

/**
 * This class is responsible for parsing a manifest yaml string and evaluating any references
 *
 * Note: This class contains code purpose built to resolve $ref and #/ references in a yaml file.
 * and should be kept inline with its sibling implementation in the airbyte repo.
 *
 * https://github.com/airbytehq/airbyte/blob/master/airbyte-cdk/python/airbyte_cdk/sources/declarative/parsers/manifest_reference_resolver.py
 */
class ManifestParser(
  rawManifestYaml: String,
) {
  var manifestMap: Map<String, Any> = processManifestYaml(rawManifestYaml)

  val streams: List<Map<String, Any>>
    get() {
      // if no streams throw manifest exception
      if (!manifestMap.containsKey("streams")) {
        throw ManifestParserException("Manifest provided is invalid. Missing streams field.")
      }

      @Suppress("UNCHECKED_CAST")
      return manifestMap["streams"] as List<Map<String, Any>>
    }

  val spec: Map<String, Any>
    get() {
      // if no spec throw manifest exception
      if (!manifestMap.containsKey("spec")) {
        throw ManifestParserException("Manifest provided is invalid. Missing spec field.")
      }

      @Suppress("UNCHECKED_CAST")
      return manifestMap["spec"] as Map<String, Any>
    }

  private fun processManifestYaml(rawYamlString: String): Map<String, Any> {
    val yaml = removeEscapes(rawYamlString)
    val yamlMap = loadYaml(yaml)
    return processManifestMap(yamlMap)
  }

  /**
   * Removes escape quote characters from the serialized string
   * e.g. \\" -> '
   *
   * Note: This is due to the way the yaml in the FE library serializes escaped strings
   */
  private fun unEscapeQuotes(serializedString: String): String {
    // Handle escaped quotes in the string
    // \\" -> '
    return serializedString.replace("\\\\\"", "\"")
  }

  private fun removeEscapes(serializedYaml: String): String {
    var deserializedYaml = unEscapeQuotes(serializedYaml)
    deserializedYaml = deserializedYaml.replace("\\n", "\n")
    return deserializedYaml
  }

  private fun processManifestMap(manifest: Map<String, Any>): Map<String, Any> = evaluateNode(manifest, manifest, mutableSetOf()) as Map<String, Any>

  private fun loadYaml(yaml: String): Map<String, Any> =
    try {
      Yaml().load(yaml) as Map<String, Any>
    } catch (e: Exception) {
      throw ManifestParserException("Manifest provided is not valid yaml. error: ${e.message}", e)
    }

  @Suppress("UNCHECKED_CAST")
  private fun evaluateNode(
    node: Any?,
    manifest: Map<String, Any>,
    visited: MutableSet<String>,
  ): Any? =
    when (node) {
      is Map<*, *> -> evaluateMapNode(node as Map<String, Any>, manifest, visited)
      is List<*> -> node.map { evaluateNode(it, manifest, visited) }
      else -> node
    }

  private fun evaluateMapNode(
    mapNode: Map<String, Any>,
    manifest: Map<String, Any>,
    visited: MutableSet<String>,
  ): Any? {
    if (REF_TAG in mapNode) {
      return handleReference(mapNode[REF_TAG] as String, manifest, visited)
    }

    return mapNode.mapValues { (_, value) ->
      if (value is String && value.startsWith(REF_VALUE)) {
        handleReference(value, manifest, visited)
      } else {
        evaluateNode(value, manifest, visited)
      }
    }
  }

  /**
   * Parses an incoming manifest and dereferences any defined references
   *
   * References can be defined using a `#/` string.
   *
   * Example:
   * ```
   * key: 1234
   * reference: "#/key"
   * ```
   * will produce the following definition:
   * ```
   * key: 1234
   * reference: 1234
   * ```
   *
   * This also works with objects:
   * ```
   * key_value_pairs:
   *   k1: v1
   *   k2: v2
   * same_key_value_pairs: "#/key_value_pairs"
   * ```
   * will produce the following definition:
   * ```
   * key_value_pairs:
   *   k1: v1
   *   k2: v2
   * same_key_value_pairs:
   *   k1: v1
   *   k2: v2
   * ```
   *
   * The `$ref` keyword can be used to refer to an object and enhance it with additional key-value pairs.
   * ```
   * key_value_pairs:
   *   k1: v1
   *   k2: v2
   * same_key_value_pairs:
   *   $ref: "#/key_value_pairs"
   *   k3: v3
   * ```
   * will produce the following definition:
   * ```
   * key_value_pairs:
   *   k1: v1
   *   k2: v2
   * same_key_value_pairs:
   *   k1: v1
   *   k2: v2
   *   k3: v3
   * ```
   *
   * References can also point to nested values. Nested references can be ambiguous because a key containing a dot (`.`)
   * could be interpreted in different ways.
   *
   * Example 1:
   * ```
   * dict:
   *   limit: 50
   * limit_ref: "#/dict/limit"
   * ```
   * will produce the following definition:
   * ```
   * dict:
   *   limit: 50
   * limit_ref: 50
   * ```
   *
   * Example 2:
   * ```
   * nested:
   *   path: "first one"
   * nested/path: "uh oh"
   * value: "#/nested/path"
   * ```
   * will produce the following definition:
   * ```
   * nested:
   *   path: "first one"
   * nested/path: "uh oh"
   * value: "uh oh"
   * ```
   *
   * To resolve ambiguity, the parser looks for the reference key at the top level first, then traverses the structures
   * downward until it finds a key with the given path or until there is nothing left to traverse.
   */
  private fun handleReference(
    ref: String,
    manifest: Map<String, Any>,
    visited: MutableSet<String>,
  ): Any? {
    if (visited.contains(ref)) {
      throw CircularReferenceException(ref)
    }

    visited.add(ref)
    val refNode = resolveReference(ref, manifest)
    val evaluatedNode = evaluateNode(refNode, manifest, visited)
    visited.remove(ref)
    return evaluatedNode
  }

  @Suppress("UNCHECKED_CAST")
  private fun resolveReference(
    ref: String,
    manifest: Map<String, Any>,
  ): Any {
    val path = ref.trimStart('#').split('/').filter { it.isNotEmpty() }
    var current: Any = manifest

    for (key in path) {
      current = (current as? Map<String, Any>)?.get(key)
        ?: throw UndefinedReferenceException(path.joinToString("/"), ref)
    }
    return current
  }
}
