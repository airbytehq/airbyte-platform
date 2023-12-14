/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.secretsmanager.caching.SecretCache
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException
import com.amazonaws.services.secretsmanager.model.CreateSecretRequest
import com.amazonaws.services.secretsmanager.model.DeleteSecretRequest
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import com.amazonaws.services.secretsmanager.model.Tag
import com.amazonaws.services.secretsmanager.model.UpdateSecretRequest
import com.google.common.base.Preconditions
import io.airbyte.config.AwsRoleSecretPersistenceConfig
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
    val existingSecret =
      try {
        read(coordinate)
      } catch (e: AWSSecretsManagerException) {
        // We use tags to control access to secrets.
        // The AWS SDK doesn't differentiate between role access exceptions and secret not found exceptions to prevent leaking information.
        // Because of this we catch the exception and if it's due to the assumed-role not having access, we just ignore it and proceed.
        // In theory, the secret should not exist, and we will go straight to attempting to create which is safe because:
        // 1. Update and create are distinct actions and we can't create over an already existing secret so we should get an error and no-op
        // 2. If the secret does exist, we will get an error and no-op
        if (e.localizedMessage.contains("assumed-role")) {
          logger.info { "AWS exception caught - Secret ${coordinate.coordinateBase} not found" }
          ""
        } else {
          throw e
        }
      }
    if (existingSecret.isNotEmpty()) {
      logger.debug { "Secret ${coordinate.coordinateBase} found updating payload." }
      val request =
        UpdateSecretRequest()
          .withSecretId(coordinate.coordinateBase)
          .withSecretString(payload)
          .withDescription("Airbyte secret.")

      if (awsClient.serializedConfig?.kmsKeyArn != null) {
        request.withKmsKeyId(awsClient.serializedConfig?.kmsKeyArn)
      }

      // No tags on update

      awsClient.client.updateSecret(request)
    } else {
      logger.debug { "Secret ${coordinate.coordinateBase} not found, creating a new one." }
      val secretRequest =
        CreateSecretRequest()
          .withName(coordinate.coordinateBase)
          .withSecretString(payload)
          .withDescription("Airbyte secret.")

      if (awsClient.serializedConfig?.kmsKeyArn != null) {
        secretRequest.withKmsKeyId(awsClient.serializedConfig?.kmsKeyArn)
      }

      if (awsClient.serializedConfig?.tagKey != null) {
        secretRequest.withTags(Tag().withKey(awsClient.serializedConfig?.tagKey).withValue("true"))
      }

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
class AwsClient(
  @Value("\${airbyte.secret.store.aws.access-key}") private val awsAccessKey: String,
  @Value("\${airbyte.secret.store.aws.secret-key}") private val awsSecretKey: String,
  @Value("\${airbyte.secret.store.aws.region}") private val awsRegion: String,
) {
  // values coming from a passed in SecretPersistenceConfig
  var serializedConfig: AwsRoleSecretPersistenceConfig? = null
  private lateinit var roleArn: String
  private lateinit var externalId: String
  private lateinit var region: String

  constructor(serializedConfig: AwsRoleSecretPersistenceConfig, airbyteAccessKey: String, airbyteSecretKey: String) :
    this(airbyteAccessKey, airbyteSecretKey, serializedConfig.awsRegion) {
    this.serializedConfig = serializedConfig
    this.roleArn = serializedConfig.roleArn
    this.externalId = serializedConfig.externalId
    this.region = serializedConfig.awsRegion
  }

  val client: AWSSecretsManager by lazy {
    // If credentials are part of this config, specify them. Otherwise,
    // let the SDK's default credential provider take over.
    if (serializedConfig == null) {
      logger.debug { "fetching access key/secret key based AWS secret manager" }
      AWSSecretsManagerClientBuilder.standard()
        .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(awsAccessKey, awsSecretKey)))
        .withRegion(awsRegion)
        .build()
    } else {
      logger.debug { "fetching role based AWS secret manager" }
      val credentialsProvider =
        STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "airbyte")
          .withExternalId(externalId)
          .build()
      AWSSecretsManagerClientBuilder.standard()
        .withCredentials(credentialsProvider)
        .withRegion(Regions.fromName(region))
        .build()
    }
  }
}

@Singleton
class AwsCache(private val awsClient: AwsClient) {
  val cache: SecretCache by lazy {
    SecretCache(awsClient.client)
  }
}
