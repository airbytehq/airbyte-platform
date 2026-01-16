/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.rpc.NotFoundException
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.secretmanager.v1.ListSecretVersionsRequest
import com.google.cloud.secretmanager.v1.ProjectName
import com.google.cloud.secretmanager.v1.Replication
import com.google.cloud.secretmanager.v1.Secret
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings
import com.google.cloud.secretmanager.v1.SecretName
import com.google.cloud.secretmanager.v1.SecretPayload
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.protobuf.ByteString
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig
import io.airbyte.micronaut.runtime.SECRET_MANAGER_GOOGLE
import io.airbyte.micronaut.runtime.SECRET_PERSISTENCE
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.Instant

private val logger = KotlinLogging.logger {}

/**
 * Uses Google Secret Manager (https://cloud.google.com/secret-manager) as a K/V store to access
 * secrets. In the future we will likely want to introduce more granular permission handling here.
 *
 * It's important to note that we are not making use of the versioning feature of Google Secret
 * Manager. This is for a few reasons: 1. There isn't a clean interface for getting the most recent
 * version. 2. Version writes must be sequential. This means that if we wanted to move between
 * secrets management platforms such as Hashicorp Vault and GSM, we would need to create secrets in
 * order (or depending on our retention for the secrets pretend to insert earlier versions).
 */
@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_GOOGLE$")
@Named("secretPersistence")
class GoogleSecretManagerPersistence(
  private val gsmClient: GoogleSecretManagerClient,
  private val metricClient: MetricClient,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String = gsmClient.getSecret(coordinate)

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    writeWithExpiry(coordinate, payload)
  }

  override fun writeWithExpiry(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
    expiry: Instant?,
  ) {
    if (read(coordinate).isEmpty()) {
      val expTag = listOf(MetricAttribute(MetricTags.EXPIRE_SECRET, (expiry != null).toString()))
      metricClient.count(metric = OssMetricsRegistry.CREATE_SECRET_DEFAULT_STORE, attributes = expTag.toTypedArray())

      logger.info { "GoogleSecretManagerPersistence createSecret coordinate=$coordinate expiry=$expiry" }
      gsmClient.createSecret(coordinate, payload, expiry)
    } else {
      gsmClient.addSecretVersion(coordinate, payload)
      logger.warn {
        val secretName = gsmClient.secretNameForCoordinate(coordinate)
        "Added a new version to a secret with existing versions name=$secretName coordinate=$coordinate"
      }
    }
  }

  override fun delete(coordinate: AirbyteManagedSecretCoordinate) = gsmClient.deleteSecret(coordinate)

  override fun disable(coordinate: AirbyteManagedSecretCoordinate) = gsmClient.disableAllSecretVersions(coordinate)
}

data class GoogleSecretsManagerRuntimeConfig(
  val projectId: String,
  val gcpCredentials: String,
  val region: String?,
) {
  companion object {
    fun fromSecretPersistenceConfig(config: SecretPersistenceConfig): GoogleSecretsManagerRuntimeConfig {
      // TODO: We should unmarshal this into a typed object instead of relying on the `configuration` map
      return GoogleSecretsManagerRuntimeConfig(
        projectId = config.configuration["gcpProjectId"]!!,
        gcpCredentials = config.configuration["gcpCredentialsJson"]!!,
        region = config.configuration["region"],
      )
    }
  }
}

/**
 * The "latest" alias is a magic string that gives you access to the latest secret without
 * explicitly specifying the version. For more info see:
 * https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets#access
 */
const val LATEST_VERSION = "latest"

interface GoogleSecretManagerClient {
  fun getClient(): SecretManagerServiceClient

  fun getRegion(): String?

  fun getProjectId(): String

  fun getReplicationPolicy(): Replication =
    Replication
      .newBuilder()
      .setAutomatic(Replication.Automatic.newBuilder().build())
      .build()

  fun getSecret(coordinate: SecretCoordinate): String {
    try {
      val region = getRegion()
      val projectId = getProjectId()
      val secretVersionName =
        if (!getRegion().isNullOrBlank()) {
          // For regional secrets, construct the resource name with location
          val resourceName = "projects/$projectId/locations/$region/secrets/${coordinate.fullCoordinate}"
          SecretVersionName.parse(resourceName)
        } else {
          SecretVersionName.of(projectId, coordinate.fullCoordinate, LATEST_VERSION)
        }
      val response = getClient().accessSecretVersion(secretVersionName)
      return response.payload.data.toStringUtf8()
    } catch (_: NotFoundException) {
      logger.warn { "Unable to locate secret for coordinate ${coordinate.fullCoordinate}." }
      return ""
    } catch (e: Exception) {
      logger.error(e) { "Unable to read secret for coordinate ${coordinate.fullCoordinate}. " }
      return ""
    }
  }

  fun secretNameForCoordinate(coordinate: SecretCoordinate): SecretName {
    val region = getRegion()
    val projectId = getProjectId()
    val secretName =
      if (!region.isNullOrBlank()) {
        SecretName.parse("projects/$projectId/locations/$region/secrets/${coordinate.fullCoordinate}")
      } else {
        SecretName.of(projectId, coordinate.fullCoordinate)
      }
    return secretName
  }

  fun secretParent(): String {
    val region = getRegion()
    val projectId = getProjectId()
    val parent =
      if (!region.isNullOrBlank()) {
        // for regional secrets, use the location as the parent
        "projects/$projectId/locations/$region"
      } else {
        ProjectName.of(projectId).toString()
      }

    return parent
  }

  fun createSecret(
    coordinate: SecretCoordinate,
    payload: String,
    expiry: Instant?,
  ) {
    val secretBuilder = Secret.newBuilder().setReplication(getReplicationPolicy())

    // set the expiry if specified
    expiry?.let {
      val expireTime =
        com.google.protobuf.Timestamp
          .newBuilder()
          .setSeconds(it.epochSecond)
          .build()
      secretBuilder.setExpireTime(expireTime)
    }

    // create the secret
    getClient().createSecret(secretParent(), coordinate.fullCoordinate, secretBuilder.build())

    // add a new version
    try {
      addSecretVersion(coordinate, payload)
    } catch (e: Exception) {
      // if we fail to add a version, then delete the secret
      deleteSecret(coordinate)
      throw e
    }
  }

  fun deleteSecret(coordinate: SecretCoordinate) {
    try {
      getClient().deleteSecret(secretNameForCoordinate(coordinate))
    } catch (_: NotFoundException) {
      logger.warn { "Tried to delete coordinate ${coordinate.fullCoordinate}, but it was already deleted." }
      return
    }
  }

  fun addSecretVersion(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    val secretPayload = SecretPayload.newBuilder().setData(ByteString.copyFromUtf8(payload)).build()
    getClient().addSecretVersion(secretNameForCoordinate(coordinate), secretPayload)
  }

  fun disableAllSecretVersions(coordinate: SecretCoordinate) {
    val request = ListSecretVersionsRequest.newBuilder().setParent(secretParent()).build()
    val response = getClient().listSecretVersions(request)
    response.iteratePages().forEach { page ->
      page.response.versionsList.forEach { version ->
        getClient().disableSecretVersion(version.name)
      }
    }
  }

  companion object {
    internal fun clientForCredentials(
      credentialsJson: String,
      region: String?,
    ): SecretManagerServiceClient {
      val credentialsByteStream = ByteArrayInputStream(credentialsJson.toByteArray(StandardCharsets.UTF_8))
      val credentials =
        ServiceAccountCredentials
          .fromStream(
            credentialsByteStream,
          ).createScoped(listOf("https://www.googleapis.com/auth/cloud-platform"))

      val settingsBuilder =
        SecretManagerServiceSettings
          .newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(credentials))

      // Only set regional endpoint if region is specified and not empty
      if (!region.isNullOrBlank()) {
        val apiEndpoint = "secretmanager.$region.rep.googleapis.com:443"
        settingsBuilder.endpoint = apiEndpoint
        logger.info { "Using regional Google Secret Manager endpoint: $apiEndpoint" }
      } else {
        logger.info { "Using global Google Secret Manager endpoint" }
      }

      return SecretManagerServiceClient.create(settingsBuilder.build())
    }

    fun fromRuntimeConfig(config: GoogleSecretsManagerRuntimeConfig): GoogleSecretManagerClient =
      RuntimeGoogleSecretManagerClient(
        delegateClient = clientForCredentials(config.gcpCredentials, config.region),
        config = config,
      )
  }
}

class RuntimeGoogleSecretManagerClient(
  private val delegateClient: SecretManagerServiceClient,
  private val config: GoogleSecretsManagerRuntimeConfig,
) : GoogleSecretManagerClient {
  override fun getClient(): SecretManagerServiceClient = delegateClient

  override fun getProjectId(): String = config.projectId

  override fun getRegion(): String? = config.region
}

@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_GOOGLE$")
class SystemGoogleSecretManagerClient(
  private val config: AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig,
) : GoogleSecretManagerClient {
  private val delegateClient: SecretManagerServiceClient by lazy {
    GoogleSecretManagerClient.clientForCredentials(config.credentials, config.region)
  }

  override fun getClient(): SecretManagerServiceClient = delegateClient

  override fun getProjectId(): String = config.projectId

  override fun getRegion(): String? = config.region
}
