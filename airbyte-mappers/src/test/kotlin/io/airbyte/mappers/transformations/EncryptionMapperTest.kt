/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import TEST_OBJECT_MAPPER
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName.ENCRYPTION
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.TestRecordAdapter
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.AesMode
import io.airbyte.config.mapper.configs.AesPadding
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.RsaEncryptionConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import toPrettyJsonString
import java.security.KeyPairGenerator
import java.security.PrivateKey
import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

private val logger = KotlinLogging.logger {}

@OptIn(ExperimentalStdlibApi::class)
class EncryptionMapperTest {
  companion object {
    const val SANITY_CHECK_KEY = "sanity_check"
    const val SANITY_CHECK_VALUE = "value"
  }

  private lateinit var encryptionMapper: EncryptionMapper

  @BeforeEach
  fun setup() {
    encryptionMapper = EncryptionMapper(TEST_OBJECT_MAPPER)
  }

  @Test
  fun `test spec`() {
    val jsonString = toPrettyJsonString(encryptionMapper.spec().jsonSchema())
    // spot checking some values
    logger.info { jsonString }

    // making sure required fields are defined
    assertTrue(jsonString.contains("oneOf"))

    // oneOf for the encryption config definitions
    assertTrue(jsonString.contains("required"))

    // some algo names that should appear as constants
    assertTrue(jsonString.contains("AES"))
    assertTrue(jsonString.contains("RSA"))
  }

  @Test
  fun `test spec secret definition`() {
    val jsonSchema = encryptionMapper.spec().jsonSchema()
    assertTrue(
      jsonSchema["properties"]["config"]["oneOf"].any {
        it["properties"]["key"]["airbyte_secret"].asText() == "true"
      },
    )
  }

  @Test
  fun `test hydrated secret serde`() {
    val config =
      AesEncryptionConfig(
        algorithm = EncryptionConfig.ALGO_AES,
        targetField = "target",
        mode = AesMode.CBC,
        padding = AesPadding.PKCS5Padding,
        key = AirbyteSecret.Hydrated("hydrated secret"),
      )
    val serializedConfig = TEST_OBJECT_MAPPER.writeValueAsString(config)
    logger.info { serializedConfig }
    val result = TEST_OBJECT_MAPPER.readValue(serializedConfig, EncryptionConfig::class.java) as AesEncryptionConfig
    assertEquals(AirbyteSecret.Hydrated("hydrated secret"), result.key)
  }

  @Test
  fun `test non-hydrated secret serde`() {
    val config =
      AesEncryptionConfig(
        algorithm = EncryptionConfig.ALGO_AES,
        targetField = "target",
        mode = AesMode.CBC,
        padding = AesPadding.PKCS5Padding,
        key = AirbyteSecret.Reference("non-hydrated secret"),
      )
    val serializedConfig = TEST_OBJECT_MAPPER.writeValueAsString(config)
    logger.info { serializedConfig }
    val result = TEST_OBJECT_MAPPER.readValue(serializedConfig, EncryptionConfig::class.java) as AesEncryptionConfig
    assertEquals(AirbyteSecret.Reference("non-hydrated secret"), result.key)
  }

  @Test
  fun `test hydrated secret deserialization`() {
    val config =
      """
      {
        "algorithm": "AES",
        "targetField": "column_name",
        "fieldNameSuffix": "_suffix",
        "mode": "CFB",
        "padding": "PKCS5Padding",
        "key": "hydrated key"
      }
      """.trimIndent()
    val encryptionConfig =
      encryptionMapper.spec().deserialize(
        configuredMapper =
          ConfiguredMapper(
            "encryption",
            TEST_OBJECT_MAPPER.readValue(
              config,
              JsonNode::class.java,
            ),
          ),
      )
    assertEquals("hydrated key", ((encryptionConfig.config as AesEncryptionConfig).key as AirbyteSecret.Hydrated).value)
  }

  @Test
  fun `test non-hydrated secret deserialization`() {
    val config =
      """
      {
        "algorithm": "AES",
        "targetField": "column_name",
        "fieldNameSuffix": "_suffix",
        "mode": "OFB",
        "padding": "NoPadding",
        "key": {"_secret": "my secret reference"}
      }
      """.trimIndent()
    val encryptionConfig =
      encryptionMapper.spec().deserialize(
        configuredMapper = ConfiguredMapper("encryption", TEST_OBJECT_MAPPER.readValue(config, JsonNode::class.java)),
      )
    assertEquals("my secret reference", ((encryptionConfig.config as AesEncryptionConfig).key as AirbyteSecret.Reference).reference)
  }

  @Test
  fun `test fields get nulled out`() {
    val nullTestFieldName = "nulltest"
    val config =
      EncryptionMapperConfig(
        config =
          AesEncryptionConfig(
            algorithm = "something that will fail",
            targetField = nullTestFieldName,
            fieldNameSuffix = null,
            mode = AesMode.CBC,
            padding = AesPadding.NoPadding,
            key = AirbyteSecret.Hydrated("magic"),
          ),
      )

    val testRecord = createRecord(nullTestFieldName, nullTestFieldName)
    encryptionMapper.map(config, testRecord)

    verifyRecordInvariant(testRecord).also {
      assertFalse(it.has(nullTestFieldName))
      val testRecord = it as TestRecordAdapter
      assertEquals(nullTestFieldName, testRecord.changes.first().fieldName)
      assertEquals(
        AirbyteRecordMessageMetaChange.Change.NULLED,
        AirbyteRecordMessageMetaChange.Change.fromValue(
          testRecord.changes
            .first()
            .change.name,
        ),
      )
      assertEquals(
        AirbyteRecordMessageMetaChange.Reason.PLATFORM_SERIALIZATION_ERROR,
        AirbyteRecordMessageMetaChange.Reason.fromValue(
          testRecord.changes
            .first()
            .reason.name,
        ),
      )
    }
  }

  @Test
  fun `testing aes options`() {
    AesMode.entries.forEach { mode ->
      val validConfigCount: Int =
        AesPadding.entries
          .map { padding ->
            // verify that if schema rejects config for which we cannot instantiate a Cipher
            try {
              Cipher.getInstance("AES/$mode/$padding")
              assertDoesNotThrow { runTestSchemaForAES(mode, padding) }
              return@map 1
            } catch (_: Exception) {
              assertThrows<EncryptionConfigException> { runTestSchemaForAES(mode, padding) }
              return@map 0
            }
          }.sum()

      // Making sure that each mode has at least one valid config
      assertTrue(validConfigCount > 0, "No valid config found for $mode")
    }
  }

  @Test
  fun `testing aes schema rejects invalid key`() {
    assertThrows<EncryptionConfigException> {
      runTestSchemaForAES(AesMode.CBC, AesPadding.PKCS5Padding, "invalid key")
    }
  }

  @Test
  fun `testing aes schema accepts valid key`() {
    assertDoesNotThrow {
      runTestSchemaForAES(AesMode.CBC, AesPadding.PKCS5Padding, "2b7e151628aed2a6abf7158809cf4f3c")
    }
  }

  @Test
  fun `testing aes schema does not fail on secret references`() {
    assertDoesNotThrow {
      runTestSchemaForAES(AesMode.CBC, AesPadding.PKCS5Padding)
    }
  }

  private fun runTestSchemaForAES(
    mode: AesMode,
    padding: AesPadding,
    key: String? = null,
  ) {
    encryptionMapper.schema(
      EncryptionMapperConfig(
        name = ENCRYPTION,
        config =
          AesEncryptionConfig(
            algorithm = EncryptionConfig.ALGO_AES,
            targetField = "field",
            mode = mode,
            padding = padding,
            key = if (key != null) AirbyteSecret.Hydrated(key) else AirbyteSecret.Reference("some ref"),
          ),
      ),
      SlimStream(listOf(Field("field", FieldType.STRING))),
    )
  }

  @Test
  fun `testing aes encryption`() {
    val keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
    val key = keyFactory.generateSecret(PBEKeySpec("my secret".toCharArray(), "salt".toByteArray(), 65536, 256))
    val aesConfig =
      AesEncryptionConfig(
        algorithm = "AES",
        targetField = "testField",
        fieldNameSuffix = "_encrypted",
        mode = AesMode.CBC,
        padding = AesPadding.PKCS5Padding,
        key = AirbyteSecret.Hydrated(key.encoded.toHexString()),
      )
    val config =
      EncryptionMapperConfig(
        config = aesConfig,
      )

    val testRecord = createRecord("testField", "something")
    encryptionMapper.map(config, testRecord)

    verifyRecordInvariant(testRecord).also {
      assertTrue(it.has("testField_encrypted"))
      assertFalse(it.has("testField"))

      val decryptedValue = decryptAES(it.get("testField_encrypted").asString(), aesConfig, key)
      assertEquals("something", decryptedValue)
    }
  }

  @Test
  fun `test in-place encryption`() {
    val keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
    val key = keyFactory.generateSecret(PBEKeySpec("my secret".toCharArray(), "salt".toByteArray(), 65536, 256))
    val aesConfig =
      AesEncryptionConfig(
        algorithm = "AES",
        targetField = "testInPlace",
        fieldNameSuffix = null,
        mode = AesMode.CBC,
        padding = AesPadding.PKCS5Padding,
        key = AirbyteSecret.Hydrated(key.encoded.toHexString()),
      )
    val config =
      EncryptionMapperConfig(
        config = aesConfig,
      )

    val testRecord = createRecord("testInPlace", "something to test")
    encryptionMapper.map(config, testRecord)

    verifyRecordInvariant(testRecord).also {
      assertTrue(it.has("testInPlace"))

      val decryptedValue = decryptAES(it.get("testInPlace").asString(), aesConfig, key)
      assertEquals("something to test", decryptedValue)
    }
  }

  @Test
  fun `testing rsa encryption`() {
    val keyGenerator = KeyPairGenerator.getInstance("RSA")
    keyGenerator.initialize(2048)
    val keyPair = keyGenerator.generateKeyPair()
    val rsaConfig =
      RsaEncryptionConfig(
        algorithm = "RSA",
        targetField = "testRsa",
        fieldNameSuffix = "_encrypted",
        publicKey = keyPair.public.encoded.toHexString(),
      )
    val config =
      EncryptionMapperConfig(
        config = rsaConfig,
      )

    val testRecord = createRecord("testRsa", "to encrypt")
    encryptionMapper.map(config, testRecord)

    verifyRecordInvariant(testRecord).also {
      assertTrue(it.has("testRsa_encrypted"))
      assertFalse(it.has("testRsa"))

      val decryptedValue = decryptRSA(it.get("testRsa_encrypted").asString(), rsaConfig, keyPair.private)
      assertEquals("to encrypt", decryptedValue)
    }
  }

  @Test
  fun `testing rsa encryption schema rejects config with invalid key`() {
    val rsaConfig =
      RsaEncryptionConfig(
        algorithm = "RSA",
        targetField = "test",
        fieldNameSuffix = "_encrypted",
        publicKey = "invalid key",
      )
    val config =
      EncryptionMapperConfig(
        config = rsaConfig,
      )

    assertThrows<EncryptionConfigException> {
      encryptionMapper.schema(
        config,
        SlimStream(listOf(Field("test", FieldType.STRING))),
      )
    }
  }

  private fun verifyRecordInvariant(record: AirbyteRecord): AirbyteRecord {
    assertEquals(SANITY_CHECK_VALUE, record.get(SANITY_CHECK_KEY).asString())
    return record
  }

  private fun createRecord(
    key: String,
    value: Any,
  ): AirbyteRecord =
    TestRecordAdapter(
      StreamDescriptor().withName("stream").withNamespace("ns"),
      mutableMapOf(key to value, SANITY_CHECK_KEY to SANITY_CHECK_VALUE),
    )

  private fun decryptAES(
    encrypted: String,
    config: AesEncryptionConfig,
    key: SecretKey,
  ): String {
    val encryptedBytes = encrypted.hexToByteArray()

    val cipher = Cipher.getInstance("${config.algorithm}/${config.mode}/${config.padding}")
    val iv = ByteArray(16)
    System.arraycopy(encryptedBytes, 0, iv, 0, iv.size)
    cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key.encoded, "AES"), IvParameterSpec(iv))
    val cipherText = ByteArray(encryptedBytes.size - 16)
    System.arraycopy(encryptedBytes, 16, cipherText, 0, cipherText.size)

    return cipher.doFinal(cipherText).toString(Charsets.UTF_8)
  }

  private fun decryptRSA(
    encrypted: String,
    config: RsaEncryptionConfig,
    key: PrivateKey,
  ): String {
    val encryptedBytes = encrypted.hexToByteArray()

    val cipher = Cipher.getInstance(config.algorithm)
    cipher.init(Cipher.DECRYPT_MODE, key)

    return cipher.doFinal(encryptedBytes).toString(Charsets.UTF_8)
  }
}
