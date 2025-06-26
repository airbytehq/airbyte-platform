/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.secretsmanager.caching.SecretCache
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException
import com.amazonaws.services.secretsmanager.model.CreateSecretRequest
import com.amazonaws.services.secretsmanager.model.DeleteSecretRequest
import com.amazonaws.services.secretsmanager.model.InvalidRequestException
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import com.amazonaws.services.secretsmanager.model.Tag
import com.amazonaws.services.secretsmanager.model.UpdateSecretRequest
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.google.common.base.Preconditions
import io.airbyte.config.AwsRoleSecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence.ImplementationTypes.AWS_SECRET_MANAGER
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
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^$AWS_SECRET_MANAGER$")
@Named("secretPersistence")
class AwsSecretManagerPersistence(
  private val awsClient: AwsClient,
  private val awsCache: AwsCache,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String {
    var secretString = ""
    try {
      logger.debug { "Reading secret ${coordinate.fullCoordinate}" }
      secretString = awsCache.cache.getSecretString(coordinate.fullCoordinate)
    } catch (e: ResourceNotFoundException) {
      logger.warn { "Secret ${coordinate.fullCoordinate} not found" }

      if (coordinate is AirbyteManagedSecretCoordinate) {
        // Attempt to use up old bad secrets
        // If this is just a read, this should work
        // If this is for an update, we should read the secret, return it, and then create a new correctly versioned one and delete the bad one.
        try {
          secretString = awsCache.cache.getSecretString(coordinate.coordinateBase)
        } catch (e: ResourceNotFoundException) {
          logger.warn { "Secret ${coordinate.coordinateBase} not found" }
        }
      }
    }
    return secretString
  }

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
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
        // 1. Update and create are distinct actions, and we can't create over an already existing secret, so we should get an error and no-op
        // 2. If the secret does exist, we will get an error and no-op
        if (e.localizedMessage.contains("assumed-role")) {
          logger.info { "AWS exception caught - Secret ${coordinate.fullCoordinate} not found" }
          ""
        } else {
          throw e
        }
      }
    if (existingSecret.isNotEmpty()) {
      logger.debug { "Secret ${coordinate.fullCoordinate} found updating payload." }
      val request =
        UpdateSecretRequest()
          .withSecretId(coordinate.fullCoordinate)
          .withSecretString(payload)
          .withDescription("Airbyte secret.")

      if (awsClient.serializedConfig?.kmsKeyArn != null) {
        request.withKmsKeyId(awsClient.serializedConfig?.kmsKeyArn)
      }

      // No tags on update

      try {
        awsClient.client.updateSecret(request)
        return // We can return early here because we successfully updated
      } catch (e: ResourceNotFoundException) {
        logger.warn { "Secret ${coordinate.fullCoordinate} not found for update" }
        // Otherwise we want to continue onwards.
      }
    }
    // If we couldn't find a secret to update, we should create one.
    val secretRequest =
      CreateSecretRequest()
        .withName(coordinate.fullCoordinate)
        .withSecretString(payload)
        .withDescription("Airbyte secret.")

    if (!awsClient.kmsKeyArn.isNullOrEmpty()) {
      secretRequest.withKmsKeyId(awsClient.kmsKeyArn)
    } else if (awsClient.serializedConfig?.kmsKeyArn != null) {
      secretRequest.withKmsKeyId(awsClient.serializedConfig?.kmsKeyArn)
    }

    if (awsClient.tags.isNotEmpty()) {
      for (tag in awsClient.tags) {
        secretRequest.withTags(Tag().withKey(tag.key).withValue(tag.value))
      }
    } else if (awsClient.serializedConfig?.tagKey != null) {
      secretRequest.withTags(Tag().withKey(awsClient.serializedConfig?.tagKey).withValue("true"))
    }

    awsClient.client.createSecret(secretRequest)
  }

  /**
   * Utility to clean up after integration tests.
   *
   * @param coordinate SecretCoordinate to delete.
   */
  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    // Clean up the old bad secrets we might have left behind
    deleteSecretId(coordinate.coordinateBase)
    // Clean up the actual versioned secrets we left behind
    deleteSecretId(coordinate.fullCoordinate)
  }

  private fun deleteSecretId(secretId: String) {
    try {
      awsClient.client.deleteSecret(
        DeleteSecretRequest()
          .withSecretId(secretId)
          .withForceDeleteWithoutRecovery(true),
      )
    } catch (e: ResourceNotFoundException) {
      logger.warn { "Secret $secretId not found" }
    } catch (e: InvalidRequestException) {
      // Was deleted in the UI.
      logger.warn { "Secret $secretId is already deleted" }
    }
  }
}

@Singleton
class AwsClient(
  @Value("\${airbyte.secret.store.aws.access-key}") private val awsAccessKey: String?,
  @Value("\${airbyte.secret.store.aws.secret-key}") private val awsSecretKey: String?,
  @Value("\${airbyte.secret.store.aws.region}") private val awsRegion: String,
  @Value("\${airbyte.secret.store.aws.kmsKeyArn}") val kmsKeyArn: String?,
  @Value("\${airbyte.secret.store.aws.tags}") val unparsedTags: String?,
) {
  // values coming from a passed in SecretPersistenceConfig
  var serializedConfig: AwsRoleSecretPersistenceConfig? = null

  val tags: Map<String, String> = parseTags(unparsedTags)

  private fun parseTags(tags: String?): Map<String, String> {
    // Define the regex pattern for the whole string validation
    val pattern = "^[\\w\\s._:/=+-@]+=[\\w\\s._:/=+-@]+(,\\s*[\\w\\s._:/=+-@]+=[\\w\\s._:/=+-@]+)*$".toRegex()

    // Check if unparsedTags is not null, not blank, and matches the pattern
    return if (!tags.isNullOrBlank() && pattern.matches(tags)) {
      tags.split(",").associate { part ->
        val (key, value) = part.trim().split("=")
        key to value
      }
    } else if (tags.isNullOrBlank()) {
      emptyMap() // Return an empty map if unparsedTags is null or blank
    } else {
      // If the string doesn't match the pattern, throw an error
      throw IllegalArgumentException(
        "AWS_SECRET_MANAGER_SECRET_TAGS does not match the expected format \"key1=value2,key2=value2\": $tags." +
          " Please update the AWS_SECRET_MANAGER_SECRET_TAGS env var configurations.",
      )
    }
  }

  private lateinit var roleArn: String
  private lateinit var externalId: String
  private lateinit var region: String

  // Sets data for usage with Assume Role
  constructor(serializedConfig: AwsRoleSecretPersistenceConfig, airbyteAccessKey: String, airbyteSecretKey: String) :
    this(airbyteAccessKey, airbyteSecretKey, serializedConfig.awsRegion, null, null) {
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
      AWSSecretsManagerClientBuilder
        .standard()
        .withRegion(awsRegion)
        .apply {
          if (!awsAccessKey.isNullOrEmpty() && !awsSecretKey.isNullOrEmpty()) {
            withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(awsAccessKey, awsSecretKey)))
          }
        }.build()
    } else {
      logger.debug { "fetching role based AWS secret manager" }
      val stsClient =
        AWSSecurityTokenServiceClientBuilder
          .standard()
          .withRegion(awsRegion)
          .apply {
            if (!awsAccessKey.isNullOrEmpty() && !awsSecretKey.isNullOrEmpty()) {
              withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(awsAccessKey, awsSecretKey)))
            }
          }.build()

      val credentialsProvider =
        STSAssumeRoleSessionCredentialsProvider
          .Builder(roleArn, "airbyte")
          .withStsClient(stsClient)
          .withExternalId(externalId)
          .build()

      AWSSecretsManagerClientBuilder
        .standard()
        .withCredentials(credentialsProvider)
        .withRegion(awsRegion)
        .build()
    }
  }
}

@Singleton
class AwsCache(
  private val awsClient: AwsClient,
) {
  val cache: SecretCache by lazy {
    SecretCache(awsClient.client)
  }
}
