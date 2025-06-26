/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.amazonaws.secretsmanager.caching.SecretCache
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.model.CreateSecretResult
import com.amazonaws.services.secretsmanager.model.DeleteSecretRequest
import com.amazonaws.services.secretsmanager.model.DeleteSecretResult
import com.amazonaws.services.secretsmanager.model.DescribeSecretResult
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import com.amazonaws.services.secretsmanager.model.Tag
import com.amazonaws.services.secretsmanager.model.UpdateSecretResult
import io.airbyte.config.AwsRoleSecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class AwsSecretManagerPersistenceTest {
  @Test
  fun `test reading secret from cache`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } returns secret
    every { mockCache.cache } returns mockAwsCache
    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading secret from client`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val mockResult: DescribeSecretResult = mockk()
    val mockSecretResult: GetSecretValueResult = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, AwsCache(mockClient))
    every { mockResult.versionIdsToStages } returns mapOf("version" to listOf("AWSCURRENT"))
    every { mockSecretResult.secretBinary } returns ByteBuffer.wrap(secret.toByteArray())
    every { mockSecretResult.secretBinary = any() } returns Unit
    every { mockSecretResult.versionStages } returns listOf("AWSCURRENT")
    every { mockSecretResult.setVersionStages(any()) } returns Unit
    every { mockSecretResult.secretString } returns secret
    every { mockSecretResult.clone() } returns mockSecretResult
    every { mockAwsClient.describeSecret(any()) } returns mockResult
    every { mockAwsClient.getSecretValue(any()) } returns mockSecretResult
    every { mockClient.client } returns mockAwsClient
    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading external secret from client`() {
    val secret = "secret value"
    val coordinate = SecretCoordinate.ExternalSecretCoordinate("my/external_coordinate")
    val mockClient: AwsClient = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val mockResult: DescribeSecretResult = mockk()
    val mockSecretResult: GetSecretValueResult = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, AwsCache(mockClient))
    every { mockResult.versionIdsToStages } returns mapOf("version" to listOf("AWSCURRENT"))
    every { mockSecretResult.secretBinary } returns ByteBuffer.wrap(secret.toByteArray())
    every { mockSecretResult.secretBinary = any() } returns Unit
    every { mockSecretResult.versionStages } returns listOf("AWSCURRENT")
    every { mockSecretResult.setVersionStages(any()) } returns Unit
    every { mockSecretResult.secretString } returns secret
    every { mockSecretResult.clone() } returns mockSecretResult
    every { mockAwsClient.describeSecret(any()) } returns mockResult
    every { mockAwsClient.getSecretValue(any()) } returns mockSecretResult
    every { mockClient.client } returns mockAwsClient
    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)
  }

  @Test
  fun `test reading secret from cache that is not found`() {
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } throws ResourceNotFoundException("test")
    every { mockCache.cache } returns mockAwsCache
    val result = persistence.read(coordinate)
    Assertions.assertEquals("", result)
  }

  @Test
  fun `test writing a secret via the client creates the secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } throws ResourceNotFoundException("test")
    every { mockAwsClient.createSecret(any()) } returns mockk<CreateSecretResult>()
    every { mockCache.cache } returns mockAwsCache
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null
    every { mockClient.kmsKeyArn } returns null
    every { mockClient.tags } returns emptyMap()

    persistence.write(coordinate, secret)

    verify { mockAwsClient.createSecret(any()) }
  }

  @Test
  fun `test writing a secret with tags via the client creates the secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } throws ResourceNotFoundException("test")
    every { mockAwsClient.createSecret(any()) } returns mockk<CreateSecretResult>()
    every { mockCache.cache } returns mockAwsCache
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null
    every { mockClient.kmsKeyArn } returns "testKms"
    every { mockClient.tags } returns mapOf("key1" to "value1", "key2" to "value2")

    persistence.write(coordinate, secret)

    verify {
      mockAwsClient.createSecret(
        withArg {
          assert(it.kmsKeyId.equals("testKms"))
          val expectedTags = listOf<Tag>(Tag().withKey("key1").withValue("value1"), Tag().withKey("key2").withValue("value2"))
          assert(it.tags.containsAll(expectedTags))
        },
      )
    }
  }

  @Test
  fun `test writing a secret via the client with serialized config creates the secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } throws ResourceNotFoundException("test")
    every { mockAwsClient.createSecret(any()) } returns mockk<CreateSecretResult>()
    every { mockCache.cache } returns mockAwsCache
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns AwsRoleSecretPersistenceConfig().withKmsKeyArn("testKms").withTagKey("testTag")
    every { mockClient.kmsKeyArn } returns "testKms"
    every { mockClient.tags } returns emptyMap()

    persistence.write(coordinate, secret)

    verify {
      mockAwsClient.createSecret(
        withArg {
          assert(it.kmsKeyId.equals("testKms"))
          assert(it.tags.contains(Tag().withKey("testTag").withValue("true")))
        },
      )
    }
  }

  @Test
  fun `test writing a secret via the client updates an existing secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } returns secret
    every { mockAwsClient.updateSecret(any()) } returns mockk<UpdateSecretResult>()
    every { mockCache.cache } returns mockAwsCache
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null
    every { mockClient.kmsKeyArn } returns null
    every { mockClient.tags } returns emptyMap()

    persistence.write(coordinate, secret)

    verify { mockAwsClient.updateSecret(any()) }
  }

  @Test
  fun `test deleting a secret via the client deletes the secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(any()) } returns secret
    every { mockAwsClient.deleteSecret(any()) } returns mockk<DeleteSecretResult>()
    every { mockCache.cache } returns mockAwsCache
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null
    every { mockClient.kmsKeyArn } returns null
    every { mockClient.tags } returns emptyMap()

    persistence.delete(coordinate)

    verify { mockAwsClient.deleteSecret(any<DeleteSecretRequest>()) }
  }

  @Test
  fun `test reading secret with version`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()

    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)
    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } returns secret
    every { mockCache.cache } returns mockAwsCache

    persistence.read(coordinate)

    verify(exactly = 1) { mockAwsCache.getSecretString(coordinate.fullCoordinate) }
  }

  @Test
  fun `test reading coordinate with no version falls back to base coordinate`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)

    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } throws ResourceNotFoundException("Secret not found")
    every { mockAwsCache.getSecretString(coordinate.coordinateBase) } returns secret
    every { mockCache.cache } returns mockAwsCache

    persistence.read(coordinate)

    verify(exactly = 1) { mockAwsCache.getSecretString(coordinate.fullCoordinate) }
    verify(exactly = 1) { mockAwsCache.getSecretString(coordinate.coordinateBase) }
  }

  @Test
  fun `test updating coordinate with no version falls back to base coordinate`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)

    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } throws ResourceNotFoundException("Secret not found")
    every { mockAwsCache.getSecretString(coordinate.coordinateBase) } returns secret
    every { mockCache.cache } returns mockAwsCache

    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } throws ResourceNotFoundException("Secret not found")
    every { mockAwsCache.getSecretString(coordinate.coordinateBase) } returns secret
    every { mockCache.cache } returns mockAwsCache

    persistence.read(coordinate)

    verify(exactly = 1) { mockAwsCache.getSecretString(coordinate.fullCoordinate) }
    verify(exactly = 1) { mockAwsCache.getSecretString(coordinate.coordinateBase) }
  }

  @Test
  fun `test writing secret updates secret if it exists`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)

    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } returns secret
    every { mockCache.cache } returns mockAwsCache
    every { mockAwsClient.updateSecret(any()) } returns mockk()
    every { mockAwsClient.createSecret(any()) } returns mockk()
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null

    persistence.write(coordinate, secret)

    verify(exactly = 1) { mockAwsClient.updateSecret(any()) }
    verify(exactly = 0) { mockAwsClient.createSecret(any()) }
  }

  @Test
  fun `test writing secret creates secret if it doesn't exist at all`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)

    every { mockCache.cache } returns mockAwsCache
    every { mockAwsClient.createSecret(any()) } returns mockk()
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null
    every { mockClient.kmsKeyArn } returns null
    every { mockClient.tags } returns emptyMap()

    // Simulate updating a secret that was saved as a baseCoordinate
    every { mockAwsClient.updateSecret(any()) } throws ResourceNotFoundException("Secret not found")
    // We fail to read the full coordinate OR the coordinate Base
    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } throws ResourceNotFoundException("Secret not found")
    every { mockAwsCache.getSecretString(coordinate.coordinateBase) } throws ResourceNotFoundException("Secret not found")
    persistence.write(coordinate, secret)

    verify(exactly = 0) { mockAwsClient.updateSecret(any()) }
    verify(exactly = 1) { mockAwsClient.createSecret(any()) }
  }

  @Test
  fun `test writing secret creates secret if baseCoordinate exists`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)
    val mockClient: AwsClient = mockk()
    val mockCache: AwsCache = mockk()
    val mockAwsCache: SecretCache = mockk()
    val mockAwsClient: AWSSecretsManager = mockk()
    val persistence = AwsSecretManagerPersistence(mockClient, mockCache)

    every { mockCache.cache } returns mockAwsCache
    every { mockAwsClient.createSecret(any()) } returns mockk()
    every { mockClient.client } returns mockAwsClient
    every { mockClient.serializedConfig } returns null
    every { mockClient.kmsKeyArn } returns null
    every { mockClient.tags } returns emptyMap()

    // Simulate updating a secret that was saved as a baseCoordinate
    every { mockAwsClient.updateSecret(any()) } throws ResourceNotFoundException("Secret not found")
    // We fail to read the full coordinate only
    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } throws ResourceNotFoundException("Secret not found")
    every { mockAwsCache.getSecretString(coordinate.coordinateBase) } returns secret
    persistence.write(coordinate, secret)

    verify(exactly = 1) { mockAwsClient.createSecret(any()) }
  }
}
