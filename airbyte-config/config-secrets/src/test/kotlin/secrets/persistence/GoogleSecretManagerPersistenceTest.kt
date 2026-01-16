/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse
import com.google.cloud.secretmanager.v1.Replication
import com.google.cloud.secretmanager.v1.Secret
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretName
import com.google.cloud.secretmanager.v1.SecretPayload
import com.google.cloud.secretmanager.v1.SecretVersion
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.protobuf.ByteString
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.micrometer.core.instrument.Counter
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.Instant

class GoogleSecretManagerPersistenceTest {
  @Test
  fun `test reading secret `() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockResponse: AccessSecretVersionResponse = mockk()
    every { mockResponse.payload } returns mockPayload

    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    every { mockGoogleClient.accessSecretVersion(versionName) } returns mockResponse
    every { mockGoogleClient.close() } returns Unit

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), value = any()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(spyGsmClient, mockMetric)
    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading secret that is not found`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockResponse: AccessSecretVersionResponse = mockk()
    every { mockResponse.payload } returns mockPayload

    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    every { mockGoogleClient.accessSecretVersion(versionName) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )
    every { mockGoogleClient.close() } returns Unit

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), value = any()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    val result = persistence.read(coordinate)
    Assertions.assertEquals("", result)

    assertThrows<NotFoundException> {
      mockGoogleClient.accessSecretVersion(versionName)
    }
  }

  @Test
  fun `test create new secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockResponse: AccessSecretVersionResponse = mockk()
    every { mockResponse.payload } returns mockPayload

    val mockSecret: Secret = mockk()
    every { mockSecret.name } returns secret

    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>()) } returns mockSecret
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockGoogleClient.accessSecretVersion(versionName) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    persistence.write(coordinate, secret)

    verify {
      mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>())
      mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>())
    }
  }

  @Test
  fun `test create new secret with expiry`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val expiry = Instant.now().plus(Duration.ofHours(72))
    val replicationPolicy =
      Replication
        .newBuilder()
        .setAutomatic(Replication.Automatic.newBuilder().build())
        .build()

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockSecret: Secret = mockk()
    every { mockSecret.name } returns secret

    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>()) } returns mockSecret
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockGoogleClient.accessSecretVersion(versionName) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    persistence.writeWithExpiry(coordinate, secret, expiry)

    val secretWithExpiry =
      Secret
        .newBuilder()
        .setReplication(
          replicationPolicy,
        ).setExpireTime(
          com.google.protobuf.Timestamp
            .newBuilder()
            .setSeconds(expiry.epochSecond)
            .build(),
        ).build()

    verify {
      mockGoogleClient.createSecret("projects/test-project", coordinate.fullCoordinate, secretWithExpiry)
      mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>())
    }
  }

  @Test
  fun `test write to an existing secret updates the secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockResponse: AccessSecretVersionResponse = mockk()
    every { mockResponse.payload } returns mockPayload

    val mockSecret: Secret = mockk()
    every { mockSecret.name } returns secret

    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>()) } returns mockSecret
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockGoogleClient.accessSecretVersion(versionName) } returns mockResponse

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    persistence.write(coordinate, secret)

    verify(exactly = 0) { mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>()) }
    verify {
      mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>())
    }
  }

  @Test
  fun `test exception when adding a secret version during creation will delete the new secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockResponse: AccessSecretVersionResponse = mockk()
    every { mockResponse.payload } returns mockPayload

    val mockSecret: Secret = mockk()
    every { mockSecret.name } returns secret

    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>()) } returns mockSecret
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } throws Exception()
    every { mockGoogleClient.deleteSecret(any<SecretName>()) } just Runs
    every { mockGoogleClient.close() } returns Unit
    every { mockGoogleClient.accessSecretVersion(versionName) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    assertThrows<Exception> { persistence.write(coordinate, secret) }

    verify {
      mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>())
      mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>())
      mockGoogleClient.deleteSecret(any<SecretName>())
    }
  }

  @Test
  fun `test exception when adding a secret version for existing secret does not delete secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockPayload: SecretPayload = mockk()
    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)

    val mockResponse: AccessSecretVersionResponse = mockk()
    every { mockResponse.payload } returns mockPayload

    val mockSecret: Secret = mockk()
    every { mockSecret.name } returns secret

    val versionName = SecretVersionName.of("test-project", coordinate.fullCoordinate, "latest")
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.createSecret("projects/test-project", any(), any<Secret>()) } returns mockSecret
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } throws Exception()
    every { mockGoogleClient.close() } returns Unit
    every { mockGoogleClient.accessSecretVersion(versionName) } returns mockResponse

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    assertThrows<Exception> { persistence.write(coordinate, secret) }

    verify(exactly = 0) { mockGoogleClient.deleteSecret(any<SecretName>()) }
    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
  }

  @Test
  fun `test delete secret`() {
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.deleteSecret(ofType(SecretName::class)) } just Runs

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    persistence.delete(coordinate)

    verify {
      mockGoogleClient.deleteSecret(any<SecretName>())
    }
  }

  @Test
  fun `test deleting a secret that doesn't exist swallows NotFoundException`() {
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockGoogleClient: SecretManagerServiceClient = mockk()
    every { mockGoogleClient.deleteSecret(any<SecretName>()) } throws
      NotFoundException(
        StatusRuntimeException(Status.NOT_FOUND),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )
    every { mockGoogleClient.close() } returns Unit

    mockkObject(GoogleSecretManagerClient.Companion)
    every { GoogleSecretManagerClient.clientForCredentials(any<String>(), any<String>()) } returns mockGoogleClient

    val gsmClient =
      SystemGoogleSecretManagerClient(
        AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.GoogleSecretsManagerConfig(
          projectId = "test-project",
          credentials = "{}",
          region = "",
        ),
      )

    val spyGsmClient = spyk(gsmClient)
    every { spyGsmClient.getClient() } returns mockGoogleClient

    val mockMetric: MetricClient = mockk()
    every { mockMetric.count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val persistence = GoogleSecretManagerPersistence(gsmClient, mockMetric)
    persistence.delete(coordinate)

    Assertions.assertDoesNotThrow {
      persistence.delete(coordinate)
    }

    verify {
      mockGoogleClient.deleteSecret(any<SecretName>())
    }
  }
}
