/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse
import com.google.cloud.secretmanager.v1.ProjectName
import com.google.cloud.secretmanager.v1.Secret
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.SecretName
import com.google.cloud.secretmanager.v1.SecretPayload
import com.google.cloud.secretmanager.v1.SecretVersion
import com.google.cloud.secretmanager.v1.SecretVersionName
import com.google.protobuf.ByteString
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.GoogleSecretManagerPersistence.Companion.replicationPolicy
import io.airbyte.metrics.MetricClient
import io.grpc.Status
import io.micrometer.core.instrument.Counter
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.Instant

class GoogleSecretManagerPersistenceTest {
  @Test
  fun `test reading secret from client`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } returns mockResponse
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any()) } returns mockk<Counter>()

    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading a secret that doesn't exist from the client`() {
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any()) } returns mockk<Counter>()

    Assertions.assertDoesNotThrow {
      val result = persistence.read(coordinate)
      Assertions.assertEquals("", result)
    }
  }

  @Test
  fun `test writing a secret via the client creates the secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )
    every { mockGoogleClient.createSecret(any<ProjectName>(), any<String>(), any<Secret>()) } returns mockk<Secret>()
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

    persistence.write(coordinate!!, secret)

    verify { mockGoogleClient.createSecret(any<ProjectName>(), any<String>(), any<Secret>()) }
    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
  }

  @Test
  fun `test writing a secret with expiry via the client creates the secret with expiry`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } throws
      NotFoundException(
        NullPointerException("test"),
        GrpcStatusCode.of(
          Status.Code.NOT_FOUND,
        ),
        false,
      )
    every { mockGoogleClient.createSecret(any<ProjectName>(), any<String>(), any<Secret>()) } returns mockk<Secret>()
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

    val expiry = Instant.now().plus(Duration.ofMinutes(1))
    persistence.writeWithExpiry(coordinate, secret, expiry)

    val sb =
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
    verify { mockGoogleClient.createSecret(ProjectName.of("test"), coordinate.fullCoordinate, sb) }
    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
  }

  @Test
  fun `test writing a secret via the client updates an existing secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } returns mockResponse
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

    persistence.write(coordinate, secret)

    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
  }

  @Test
  fun `test exception when adding a secret version deletes new secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)
    val mockSecret: Secret = mockk()

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockResponse.payload.data.toStringUtf8() } returns ""
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } returns mockResponse
    every { mockSecret.name } returns secret
    coEvery {
      mockGoogleClient.createSecret(
        any<ProjectName>(),
        any<String>(),
        any<Secret>(),
      )
    } returns mockSecret
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } throws RuntimeException()
    every { mockGoogleClient.deleteSecret(secret) } just Runs
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

    assertThrows<RuntimeException> { persistence.writeWithExpiry(coordinate, secret) }

    verify {
      mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>())
      mockGoogleClient.deleteSecret(secret)
    }
  }

  @Test
  fun `test exception when adding a secret version does not delete a pre-existing secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockResponse.payload.data.toStringUtf8() } returns "mocked-secret-value"
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } returns mockResponse
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } throws RuntimeException()
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockMetric.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

    assertThrows<RuntimeException> { persistence.writeWithExpiry(coordinate, secret) }

    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
    verify(exactly = 0) { mockGoogleClient.deleteSecret(any<SecretName>()) }
  }

  @Test
  fun `test deleting a secret via the client deletes the secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val mockMetric: MetricClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, null, mockClient, mockMetric)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockClient.createClient() } returns mockGoogleClient
    every { mockGoogleClient.deleteSecret(ofType(SecretName::class)) } just Runs
    every { mockGoogleClient.close() } returns Unit
    every { mockMetric.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

    persistence.delete(coordinate)

    verify { mockGoogleClient.deleteSecret(any<SecretName>()) }
  }
}
