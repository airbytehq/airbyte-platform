/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient.ListSecretVersionsPagedResponse
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings
import com.google.cloud.secretmanager.v1.SecretName
import com.google.cloud.secretmanager.v1.SecretPayload
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.protobuf.ByteString
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence.ImplementationTypes.GOOGLE_SECRET_MANAGER
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
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
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^$GOOGLE_SECRET_MANAGER$")
@Named("secretPersistence")
class GoogleSecretManagerPersistence(
  @Value("\${airbyte.secret.store.gcp.project-id}") val gcpProjectId: String,
  private val googleSecretManagerServiceClient: GoogleSecretManagerServiceClient,
  private val metricClient: MetricClient,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String {
    try {
      googleSecretManagerServiceClient.createClient().use { client ->
        val secretVersionName = SecretVersionName.of(gcpProjectId, coordinate.fullCoordinate, LATEST)
        val response = client.accessSecretVersion(secretVersionName)
        return response.payload.data.toStringUtf8()
      }
    } catch (e: NotFoundException) {
      logger.warn { "Unable to locate secret for coordinate ${coordinate.fullCoordinate}." }
      return ""
    } catch (e: Exception) {
      logger.error(e) { "Unable to read secret for coordinate ${coordinate.fullCoordinate}. " }
      return ""
    }
  }

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    writeWithExpiry(coordinate, payload)
  }

  companion object {
    /**
     * The "latest" alias is a magic string that gives you access to the latest secret without
     * explicitly specifying the version. For more info see:
     * https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets#access
     */
    const val LATEST = "latest"

    val replicationPolicy: Replication =
      Replication
        .newBuilder()
        .setAutomatic(Replication.Automatic.newBuilder().build())
        .build()
  }

  override fun writeWithExpiry(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
    expiry: Instant?,
  ) {
    googleSecretManagerServiceClient.createClient().use { client ->
      if (read(coordinate).isEmpty()) {
        val secretBuilder = Secret.newBuilder().setReplication(replicationPolicy)

        var expTag = listOf(MetricAttribute(MetricTags.EXPIRE_SECRET, "false"))
        expiry?.let {
          val expireTime =
            com.google.protobuf.Timestamp
              .newBuilder()
              .setSeconds(it.epochSecond)
              .build()
          secretBuilder.setExpireTime(expireTime)
          expTag = listOf(MetricAttribute(MetricTags.EXPIRE_SECRET, "true"))
        }

        metricClient.count(metric = OssMetricsRegistry.CREATE_SECRET_DEFAULT_STORE, attributes = expTag.toTypedArray())

        logger.info { "GoogleSecretManagerPersistence createSecret coordinate=$coordinate expiry=$expiry" }

        val secret = client.createSecret(ProjectName.of(gcpProjectId), coordinate.fullCoordinate, secretBuilder.build())
        try {
          addSecretVersion(client, coordinate, payload)
        } catch (e: Exception) {
          client.deleteSecret(secret.name)
          throw e
        }
      } else {
        addSecretVersion(client, coordinate, payload)
        logger.warn {
          "Added a new version to a secret with existing versions name=${SecretName.of(
            gcpProjectId,
            coordinate.fullCoordinate,
          )} coordinate=$coordinate"
        }
      }
    }
  }

  fun addSecretVersion(
    client: SecretManagerServiceClient,
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    val name = SecretName.of(gcpProjectId, coordinate.fullCoordinate)
    val secretPayload = SecretPayload.newBuilder().setData(ByteString.copyFromUtf8(payload)).build()
    client.addSecretVersion(name, secretPayload)
  }

  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    googleSecretManagerServiceClient.createClient().use { client ->
      val secretName = SecretName.of(gcpProjectId, coordinate.fullCoordinate)
      client.deleteSecret(secretName)
    }
  }

  override fun disable(coordinate: AirbyteManagedSecretCoordinate) {
    googleSecretManagerServiceClient.createClient().use { client ->
      val secretVersionName = SecretName.of(gcpProjectId, coordinate.fullCoordinate)
      val request = ListSecretVersionsRequest.newBuilder().setParent(secretVersionName.toString()).build()

      val response: ListSecretVersionsPagedResponse = client.listSecretVersions(request)
      response.iterateAll().forEach { secret ->
        val version = secret.name.split("/").last()
        val versionName = SecretVersionName.of(gcpProjectId, coordinate.fullCoordinate, version)
        client.disableSecretVersion(versionName)
      }
    }
  }
}

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^google_secret_manager$")
class GoogleSecretManagerServiceClient(
  @Value("\${airbyte.secret.store.gcp.credentials}") private val gcpCredentialsJson: String,
) {
  /**
   * Creates a new {@link SecretManagerServiceClient} on each invocation.
   *
   * @return A new {@link SecretManagerServiceClient} instance.
   */
  fun createClient(): SecretManagerServiceClient {
    val credentialsByteStream = ByteArrayInputStream(gcpCredentialsJson.toByteArray(StandardCharsets.UTF_8))
    val credentials = ServiceAccountCredentials.fromStream(credentialsByteStream)
    val clientSettings =
      SecretManagerServiceSettings
        .newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
        .build()
    return SecretManagerServiceClient.create(clientSettings)
  }
}
