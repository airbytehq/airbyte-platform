/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode

@JvmInline
// An actor config with secret references resolved back to coordinates
value class InlinedConfigWithSecretRefs(
  val value: JsonNode,
)

fun InlinedConfigWithSecretRefs.toConfigWithRefs(): ConfigWithSecretReferences =
  ConfigWithSecretReferences(
    this.value,
    SecretsHelpers.SecretReferenceHelpers.getReferenceMapFromConfig(this),
  )

@Deprecated(
  message = "Use InlinedConfigWithSecretRefs.toConfigWithRefs() instead once code has been converted to Kotlin",
  replaceWith = ReplaceWith("this.toConfigWithRefs()"),
)
fun buildConfigWithSecretRefsJava(config: JsonNode): ConfigWithSecretReferences =
  ConfigWithSecretReferences(
    config,
    SecretsHelpers.SecretReferenceHelpers.getReferenceMapFromConfig(
      InlinedConfigWithSecretRefs(config),
    ),
  )

fun ConfigWithSecretReferences.toInlined(): InlinedConfigWithSecretRefs =
  SecretsHelpers.SecretReferenceHelpers.inlineSecretReferences(this.originalConfig, this.referencedSecrets)
