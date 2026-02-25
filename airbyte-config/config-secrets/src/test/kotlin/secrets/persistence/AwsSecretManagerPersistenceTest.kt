/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.amazonaws.secretsmanager.caching.SecretCache
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException
import com.amazonaws.services.secretsmanager.model.CreateSecretResult
import com.amazonaws.services.secretsmanager.model.DeleteSecretResult
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import com.amazonaws.services.secretsmanager.model.Tag
import com.amazonaws.services.secretsmanager.model.UpdateSecretResult
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AwsSecretsManagerConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import secrets.persistence.SecretCoordinateException

class AwsSecretManagerPersistenceTest {
  @Test
  fun `test reading secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsCache: SecretCache = mockk()
    every { mockAwsCache.getSecretString(any()) } returns secret

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getCache() } returns mockAwsCache

    val persistence = AwsSecretManagerPersistence(spyAwsClient)
    val result = persistence.read(coordinate)
    Assertions.assertEquals(secret, result)

    // It should be found using the full coordinate (w/ version)
    verify(exactly = 1) {
      mockAwsCache.getSecretString(coordinate.fullCoordinate)
    }
    verify(exactly = 0) {
      mockAwsCache.getSecretString(coordinate.coordinateBase)
    }
  }

  @Test
  fun `test reading secret that is not found`() {
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsCache: SecretCache = mockk()

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getCache() } returns mockAwsCache

    val persistence = AwsSecretManagerPersistence(spyAwsClient)
    every { mockAwsCache.getSecretString(any()) } throws ResourceNotFoundException("test")

    val result = persistence.read(coordinate)
    Assertions.assertEquals("", result)

    // It should not be found with either coordinate
    verify(exactly = 1) {
      mockAwsCache.getSecretString(coordinate.fullCoordinate)
    }
    verify(exactly = 1) {
      mockAwsCache.getSecretString(coordinate.coordinateBase)
    }
  }

  @Test
  fun `test reading secret with no version falls back to using base coordinate`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsCache: SecretCache = mockk()
    every { mockAwsCache.getSecretString(coordinate.fullCoordinate) } throws ResourceNotFoundException("test")
    every { mockAwsCache.getSecretString(coordinate.coordinateBase) } returns secret

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getCache() } returns mockAwsCache

    val persistence = AwsSecretManagerPersistence(spyAwsClient)
    val result = persistence.read(coordinate)
    Assertions.assertEquals("secret value", result)

    verify(exactly = 1) {
      mockAwsCache.getSecretString(coordinate.fullCoordinate)
    }
    verify(exactly = 1) {
      mockAwsCache.getSecretString(coordinate.coordinateBase)
    }
  }

  @Test
  fun `test reading secret throws SecretCoordinateException when AWS auth fails`() {
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsCache: SecretCache = mockk()
    val awsException = AWSSecretsManagerException("The security token included in the request is invalid.")
    every { mockAwsCache.getSecretString(any()) } throws awsException

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getCache() } returns mockAwsCache

    val exception =
      Assertions.assertThrows(SecretCoordinateException::class.java) {
        spyAwsClient.getSecret(coordinate)
      }
    Assertions.assertSame(awsException, exception.cause)
    Assertions.assertTrue(exception.message!!.contains(coordinate.fullCoordinate))
    Assertions.assertEquals("aws", exception.secretStoreType)
  }

  @Test
  fun `test reading secret still returns empty when ResourceNotFoundException is thrown`() {
    val coordinate = SecretCoordinate.ExternalSecretCoordinate("external_secret_coordinate")

    val mockAwsCache: SecretCache = mockk()
    every { mockAwsCache.getSecretString(any()) } throws ResourceNotFoundException("not found")

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getCache() } returns mockAwsCache

    val result = spyAwsClient.getSecret(coordinate)
    Assertions.assertEquals("", result)
  }

  @Test
  fun `test writing a new secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsClient = mockk<AWSSecretsManager>()
    every { mockAwsClient.createSecret(any()) } returns mockk<CreateSecretResult>()

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getClient() } returns mockAwsClient
    every { spyAwsClient.getSecret(coordinate) } returns ""
    every { spyAwsClient.getKmsKeyArn() } returns "testKms"

    val persistence = AwsSecretManagerPersistence(spyAwsClient)
    persistence.write(coordinate, secret)
  }

  @Test
  fun `test writing a secret with tags`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsClient = mockk<AWSSecretsManager>()
    every { mockAwsClient.createSecret(any()) } returns mockk<CreateSecretResult>()

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getClient() } returns mockAwsClient
    every { spyAwsClient.getSecret(coordinate) } returns ""
    every { spyAwsClient.parseTags(any()) } returns mapOf("key1" to "value1", "key2" to "value2")
    every { spyAwsClient.getKmsKeyArn() } returns "testKms"

    val persistence = AwsSecretManagerPersistence(spyAwsClient)
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
  fun `test writing a secret with kmsKeyId`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsClient = mockk<AWSSecretsManager>()
    every { mockAwsClient.createSecret(any()) } returns mockk<CreateSecretResult>()

    val awsClient =
      SystemAwsSecretsManagerClient(
        config = AwsSecretsManagerConfig(),
      )
    val spyAwsClient = spyk(awsClient)
    every { spyAwsClient.getClient() } returns mockAwsClient
    every { spyAwsClient.getSecret(coordinate) } returns ""
    every { spyAwsClient.getKmsKeyArn() } returns "testKms"

    val persistence = AwsSecretManagerPersistence(spyAwsClient)
    persistence.write(coordinate, secret)

    verify {
      mockAwsClient.createSecret(
        withArg {
          assert(it.kmsKeyId.equals("testKms"))
        },
      )
    }
  }

  @Test
  fun `test writing to an existing secret updates the secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsClient = mockk<AWSSecretsManager>()
    every { mockAwsClient.updateSecret(any()) } returns mockk<UpdateSecretResult>()

    val mockClient: SystemAwsSecretsManagerClient = mockk()
    every { mockClient.getClient() } returns mockAwsClient
    every { mockClient.getSecret(coordinate) } returns secret
    every { mockClient.updateSecret(coordinate.fullCoordinate, secret, "Airbyte secret.") } answers { callOriginal() }
    every { mockClient.getKmsKeyArn() } returns "testKms"

    val persistence = AwsSecretManagerPersistence(mockClient)
    persistence.write(coordinate, secret)

    verify(exactly = 0) { mockAwsClient.createSecret(any()) }
    verify(exactly = 1) {
      mockAwsClient.updateSecret(
        withArg {
          assert(it.secretString.equals("secret value"))
        },
      )
    }
  }

  @Test
  fun `test deleting a secret`() {
    val secret = "secret value"
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_secret_coordinate", 1L)

    val mockAwsClient = mockk<AWSSecretsManager>()
    every { mockAwsClient.deleteSecret(any()) } returns mockk<DeleteSecretResult>()

    val mockClient: SystemAwsSecretsManagerClient = mockk()
    every { mockClient.getClient() } returns mockAwsClient
    every { mockClient.getSecret(coordinate) } returns secret
    every { mockClient.deleteSecret(coordinate.coordinateBase) } answers { callOriginal() }
    every { mockClient.deleteSecret(coordinate.fullCoordinate) } answers { callOriginal() }
    every { mockClient.getKmsKeyArn() } returns "testKms"

    val persistence = AwsSecretManagerPersistence(mockClient)
    persistence.delete(coordinate)

    verify {
      mockAwsClient.deleteSecret(
        withArg {
          assert(it.secretId.equals(coordinate.coordinateBase))
          assert(it.forceDeleteWithoutRecovery.equals(true))
        },
      )

      mockAwsClient.deleteSecret(
        withArg {
          assert(it.secretId.equals(coordinate.fullCoordinate))
          assert(it.forceDeleteWithoutRecovery.equals(true))
        },
      )
    }
  }
}
