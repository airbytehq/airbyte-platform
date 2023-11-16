/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.secretsmanager.caching.SecretCache
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.CreateSecretRequest
import com.amazonaws.services.secretsmanager.model.DeleteSecretRequest
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import com.amazonaws.services.secretsmanager.model.UpdateSecretRequest
import com.google.common.base.Preconditions
import io.airbyte.config.secrets.SecretCoordinate
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * SecretPersistence implementation for AWS Secret Manager using [Java
 * SDK](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/secretsmanager/package-summary.html) The current implementation doesn't make use of `SecretCoordinate#getVersion` as this
 * version is non-compatible with how AWS secret manager deals with versions. In AWS versions is an
 * internal idiom that can is accessible, but it's a UUID + a tag [more
 * details.](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/secretsmanager/SecretsManagerClient.html#listSecretVersionIds--)
 */
@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^aws_secret_manager$")
@Named("secretPersistence")
class AwsSecretManagerPersistence(private val awsClient: AwsClient, private val awsCache: AwsCache) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String {
    var secretString = ""
    try {
      logger.debug { "Reading secret ${coordinate.coordinateBase}" }
      secretString = awsCache.cache.getSecretString(coordinate.coordinateBase)
    } catch (e: ResourceNotFoundException) {
      logger.warn { "Secret ${coordinate.coordinateBase} not found" }
    }
    return secretString
  }

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    Preconditions.checkArgument(payload.isNotEmpty(), "Payload shouldn't be empty")
    if (read(coordinate).isNotEmpty()) {
      logger.debug { "Secret ${coordinate.coordinateBase} found updating payload." }
      val request =
        UpdateSecretRequest()
          .withSecretId(coordinate.coordinateBase)
          .withSecretString(payload)
          .withDescription("Airbyte secret.")
      awsClient.client.updateSecret(request)
    } else {
      logger.debug { "Secret ${coordinate.coordinateBase} not found, creating a new one." }
      val secretRequest =
        CreateSecretRequest()
          .withName(coordinate.coordinateBase)
          .withSecretString(payload)
          .withDescription("Airbyte secret.")
      awsClient.client.createSecret(secretRequest)
    }
  }

  /**
   * Utility to clean up after integration tests.
   *
   * @param coordinate SecretCoordinate to delete.
   */
  private fun deleteSecret(coordinate: SecretCoordinate) {
    awsClient.client.deleteSecret(
      DeleteSecretRequest()
        .withSecretId(coordinate.coordinateBase)
        .withForceDeleteWithoutRecovery(true),
    )
  }
}

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^aws_secret_manager$")
class AwsClient(
  @Value("\${airbyte.secret.store.aws.access-key}") private val awsAccessKey: String,
  @Value("\${airbyte.secret.store.aws.secret-key}") private val awsSecretKey: String,
  @Value("\${airbyte.secret.store.aws.region}") private val awsRegion: String,
) {
  val client: AWSSecretsManager by lazy {
    val builder = AWSSecretsManagerClientBuilder.standard()

    // If credentials are part of this config, specify them. Otherwise,
    // let the SDK's default credential provider take over.
    if (awsSecretKey.isNotEmpty()) {
      val credentials = BasicAWSCredentials(awsAccessKey, awsSecretKey)
      builder.withCredentials(AWSStaticCredentialsProvider(credentials))
    }
    builder.withRegion(awsRegion)
    builder.build()
  }
}

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^aws_secret_manager$")
class AwsCache(private val awsClient: AwsClient) {
  val cache: SecretCache by lazy {
    SecretCache(awsClient.client)
  }
}
