/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.bootloader.K8sSecretHelper.base64Decode
import io.airbyte.commons.random.randomAlphanumeric
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReview
import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReviewBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

const val SECRET_LENGTH = 32

@Singleton
@Requires(property = "airbyte.auth.kubernetes-secret.creation-enabled", value = "true")
class AuthKubernetesSecretInitializer(
  private val airbyteAuthConfig: AirbyteAuthConfig,
  private val kubernetesClient: KubernetesClient,
) {
  fun initializeSecrets() {
    logger.info { "Initializing auth secret in Kubernetes..." }
    K8sSecretHelper.createOrUpdateSecret(kubernetesClient, airbyteAuthConfig.kubernetesSecret.name, getSecretDataMap())
    logger.info { "Finished initializing auth secret." }
  }

  fun checkAccessToSecrets(airbyteVersion: AirbyteVersion) {
    kubernetesClient.authorization().v1().subjectAccessReview()
    val namespace: String = kubernetesClient.namespace // the namespace the client is operating in
    val review: SelfSubjectAccessReview =
      SelfSubjectAccessReviewBuilder()
        .withNewSpec()
        .withNewResourceAttributes()
        .withNamespace(namespace)
        .withVerb("create")
        .withResource("secrets")
        .endResourceAttributes()
        .endSpec()
        .build()
    val response: SelfSubjectAccessReview =
      kubernetesClient
        .authorization()
        .v1()
        .selfSubjectAccessReview()
        .create(review)
    if (!response.status.allowed) {
      throw IllegalStateException(
        """
Upgrade to version $airbyteVersion failed. As of version 1.6 of the Airbyte Platform, we require your Service Account permissions to include access to the "secrets" resource. To learn more, please visit our documentation page at https://docs.airbyte.com/enterprise-setup/upgrade-service-account.
        """.trimIndent(),
      )
    }
  }

  private fun getSecretDataMap(): Map<String, String> {
    val passwordValue =
      getOrCreateSecretValue(
        airbyteAuthConfig.kubernetesSecret.keys.instanceAdminPasswordSecretKey,
        airbyteAuthConfig.kubernetesSecret.values.instanceAdminPassword,
        randomAlphanumeric(SECRET_LENGTH),
      )
    val clientIdValue =
      getOrCreateSecretValue(
        airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientIdSecretKey,
        airbyteAuthConfig.kubernetesSecret.values.instanceAdminClientId,
        UUID.randomUUID().toString(),
      )
    val clientSecretValue =
      getOrCreateSecretValue(
        airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientSecretSecretKey,
        airbyteAuthConfig.kubernetesSecret.values.instanceAdminClientSecret,
        randomAlphanumeric(SECRET_LENGTH),
      )
    val jwtSignatureValue =
      getOrCreateSecretValue(
        airbyteAuthConfig.kubernetesSecret.keys.jwtSignatureSecretKey,
        airbyteAuthConfig.kubernetesSecret.values.jwtSignatureSecret,
        randomAlphanumeric(SECRET_LENGTH),
      )

    return mapOf(
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminPasswordSecretKey to passwordValue,
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientIdSecretKey to clientIdValue,
      airbyteAuthConfig.kubernetesSecret.keys.instanceAdminClientSecretSecretKey to clientSecretValue,
      airbyteAuthConfig.kubernetesSecret.keys.jwtSignatureSecretKey to jwtSignatureValue,
    )
  }

  private fun getOrCreateSecretValue(
    secretKey: String?,
    providedValue: String?,
    defaultValue: String,
  ): String {
    if (!providedValue.isNullOrBlank()) {
      logger.info { "Using provided value for secret key $secretKey" }
      return providedValue
    } else {
      val secret = kubernetesClient.secrets().withName(airbyteAuthConfig.kubernetesSecret.name).get()
      if (secret != null && secretKey != null && secret.data.containsKey(secretKey)) {
        logger.info { "Using existing value for secret key $secretKey" }
        return base64Decode(secret.data[secretKey]!!)
      } else {
        logger.info { "Using generated/default value for secret key $secretKey" }
        return defaultValue
      }
    }
  }
}
