/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.config.secrets.SecretCoordinate
import io.grpc.Status
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class GoogleSecretManagerPersistenceTest {
  @Test
  fun `test reading secret from client`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, mockClient)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } returns mockResponse
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient

    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading a secret that doesn't exist from the client`() {
    val projectId = "test"
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, mockClient)

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

    Assertions.assertDoesNotThrow {
      val result = persistence.read(coordinate)
      Assertions.assertEquals("", result)
    }
  }

  @Test
  fun `test writing a secret via the client creates the secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, mockClient)

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

    persistence.write(coordinate, secret)

    verify { mockGoogleClient.createSecret(any<ProjectName>(), any<String>(), any<Secret>()) }
    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
  }

  @Test
  fun `test writing a secret via the client updates an existing secret`() {
    val secret = "secret value"
    val projectId = "test"
    val coordinate = SecretCoordinate.fromFullCoordinate("secret_coordinate_v1")
    val mockClient: GoogleSecretManagerServiceClient = mockk()
    val mockGoogleClient: SecretManagerServiceClient = mockk()
    val mockResponse: AccessSecretVersionResponse = mockk()
    val mockPayload: SecretPayload = mockk()
    val persistence = GoogleSecretManagerPersistence(projectId, mockClient)

    every { mockPayload.data } returns ByteString.copyFromUtf8(secret)
    every { mockResponse.payload } returns mockPayload
    every { mockGoogleClient.accessSecretVersion(ofType(SecretVersionName::class)) } returns mockResponse
    every { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) } returns mockk<SecretVersion>()
    every { mockGoogleClient.close() } returns Unit
    every { mockClient.createClient() } returns mockGoogleClient

    persistence.write(coordinate, secret)

    verify { mockGoogleClient.addSecretVersion(any<SecretName>(), any<SecretPayload>()) }
  }
}
