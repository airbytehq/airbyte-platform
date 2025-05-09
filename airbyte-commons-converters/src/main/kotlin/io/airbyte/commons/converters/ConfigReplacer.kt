/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.AllowedHosts
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.text.StringSubstitutor
import java.net.URI

/**
 * This class takes values from a connector's configuration and uses it to fill in template-string values.
 * It substitutes strings with ${} access, e.g. "The ${animal} jumped over the ${target}" with {animal: fox, target: fence}
 */
class ConfigReplacer {
  val log = KotlinLogging.logger {}

  fun getAllowedHosts(
    allowedHosts: AllowedHosts?,
    config: JsonNode,
  ): AllowedHosts? {
    val hosts = allowedHosts?.hosts ?: return null

    val resolvedHosts = mutableListOf<String>()
    val values = mutableMapOf<String, String>()
    val jsonParser = config.traverse()

    val prefixes = mutableListOf<String>()

    while (!jsonParser.isClosed) {
      val token = jsonParser.nextToken()
      when (token) {
        JsonToken.START_OBJECT -> jsonParser.currentName()?.let { prefixes.add(it) }
        JsonToken.END_OBJECT -> runCatching { prefixes.removeLast() }
        JsonToken.FIELD_NAME -> {
          val key = jsonParser.currentName()
          // the interface for allowedHosts is dot notation, e.g. `"${tunnel_method.tunnel_host}"`
          val fullKey: String
          // the search path for JSON nodes is slash notation, e.g. `"/tunnel_method/tunnel_host"`
          val lookupKey: String

          if (prefixes.isEmpty()) {
            fullKey = key
            lookupKey = "/$key"
          } else {
            fullKey = prefixes.joinToString(separator = ".", postfix = ".$key")
            lookupKey = prefixes.joinToString(separator = "/", prefix = "/", postfix = "/$key")
          }

          var value = config.at(lookupKey).textValue()
          if (value == null) {
            config.at(lookupKey).numberValue()?.let { value = it.toString() }
          }

          value?.let { values[fullKey] = it.sanitize() }
        }
        else -> null
      }
    } // while

    val sub = StringSubstitutor(values)
    allowedHosts.hosts.forEach { host ->
      sub.replace(host).takeIf { !it.contains("\${") }?.let { resolvedHosts.add(it) }
    }

    if (resolvedHosts.isEmpty() && hosts.isNotEmpty()) {
      log.error { "All allowedHosts values are un-replaced.  Check this connector's configuration or actor definition - ${allowedHosts.hosts}" }
    }

    resolvedHosts.addAll(AlwaysAllowedHosts.hosts)

    return AllowedHosts().apply { this.hosts = resolvedHosts }
  }

  fun String.sanitize(): String =
    runCatching {
      val withProtocol =
        if (this.contains("://")) {
          this
        } else {
          "x://$this"
        }

      URI(withProtocol).toURL().host
    }.getOrElse { _ ->
      var sanitized = ""

      // some hosts will be provided from the connector config with a protocol, like ftp://site.com or mongodb+srv://cluster0.abcd1.mongodb.net
      with(this.split("://")) {
        if (this.size > 1) {
          sanitized = this[1]
        } else {
          sanitized = this[0]
        }
      }

      // some hosts might have a trailing path. We only want the first chunk in all cases (e.g. http://site.com/path/foo/bar)
      sanitized.split("/").let { sanitized = it[0] }

      // some hosts will have a username or password, like https://user:passowrd@site.com
      with(sanitized.split("@")) {
        if (this.size > 1) {
          sanitized = this[1]
        } else {
          sanitized = this[0]
        }
      }

      // remove slashes - we only want hostnames, not paths
      sanitized.replace("/", "")
    }
}

/**
 * Hosts that are connector containers are always allowed to access.
 */
object AlwaysAllowedHosts {
  @JvmStatic
  val hosts =
    listOf(
      // DataDog. See https://docs.datadoghq.com/agent/proxy/?tab=linux and change the location tabs
      "*.datadoghq.com",
      "*.datadoghq.eu",
      // Sentry. See https://docs.sentry.io/api/ for more information
      "*.sentry.io",
    )
}
