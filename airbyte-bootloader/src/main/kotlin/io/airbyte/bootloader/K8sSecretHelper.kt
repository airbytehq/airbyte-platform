/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

private val logger = KotlinLogging.logger {}

/**
 * Contains helpers for dealing with Kubernetes secrets.
 */
object K8sSecretHelper {
  /**
   * Helper function for creating or updating Kubernetes secrets from a map of key-value pairs. If
   * the secret does not exist, it will be created. If it does exist, the provided data will be
   * merged with the existing data, with the provided data taking precedence.
   *
   * Any new or updated values will be base64-encoded before being stored in the secret.
   */
  fun createOrUpdateSecret(
    kubernetesClient: KubernetesClient,
    secretName: String,
    secretData: Map<String, String>,
  ) {
    val base64EncodedSecretData = secretData.mapValues { (_, value) -> base64Encode(value) }
    val existingSecret = kubernetesClient.secrets().withName(secretName).get()
    if (existingSecret == null) {
      logger.info { "No existing secret with name $secretName was found. Creating it..." }
      val secret =
        SecretBuilder()
          .withNewMetadata()
          .withName(secretName)
          .endMetadata()
          .addToData(base64EncodedSecretData)
          .build()
      kubernetesClient.resource(secret).create()
      logger.info { "Successfully created secret $secretName" }
    } else {
      logger.info { "Secret with name $secretName already exists. Updating it..." }
      val data = existingSecret.data?.toMutableMap() ?: mutableMapOf()
      // Merge new secret data into the existing data (overriding any keys that are provided)
      base64EncodedSecretData.forEach { (key, value) ->
        data[key] = value
      }
      val updatedSecret = SecretBuilder(existingSecret).withData<String, String>(data).build()
      kubernetesClient.resource(updatedSecret).update()
      logger.info { "Successfully updated secret $secretName" }
    }
  }

  /**
   * Helper function for copying a Kubernetes secret from one namespace to another. Used for
   * new Cloud instances that operate in two separate namespaces.
   */
  fun copySecretToNamespace(
    kubernetesClient: KubernetesClient,
    secretName: String,
    sourceNamespace: String,
    targetNamespace: String,
  ) {
    val existingSecret =
      kubernetesClient
        .secrets()
        .inNamespace(targetNamespace)
        .withName(secretName)
        .get()
    if (existingSecret != null) {
      logger.info { "Secret $secretName already exists in namespace $targetNamespace. Skipping copy." }
      return
    }
    val secret =
      kubernetesClient
        .secrets()
        .inNamespace(sourceNamespace)
        .withName(secretName)
        .get()
        ?: throw IllegalStateException("Secret $secretName not found in namespace $sourceNamespace")
    val copiedSecret =
      SecretBuilder(secret)
        .withNewMetadata()
        .withName(secretName)
        .withNamespace(targetNamespace)
        .endMetadata()
        .build()
    kubernetesClient.resource(copiedSecret).create()
  }

  @OptIn(ExperimentalEncodingApi::class)
  fun base64Encode(text: String): String = Base64.encode(text.toByteArray())

  @OptIn(ExperimentalEncodingApi::class)
  fun base64Decode(encodedText: String): String = String(Base64.decode(encodedText))
}
