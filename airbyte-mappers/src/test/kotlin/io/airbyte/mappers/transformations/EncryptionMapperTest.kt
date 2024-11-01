package io.airbyte.mappers.transformations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.adapters.TestRecordAdapter
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.RsaEncryptionConfig
import io.airbyte.protocol.models.AirbyteRecordMessageMetaChange
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.security.KeyPairGenerator
import java.security.PrivateKey
import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

@OptIn(ExperimentalStdlibApi::class)
class EncryptionMapperTest {
  companion object {
    const val SANITY_CHECK_KEY = "sanity_check"
    const val SANITY_CHECK_VALUE = "value"
  }

  private lateinit var encryptionMapper: EncryptionMapper

  @BeforeEach
  fun setup() {
    encryptionMapper = EncryptionMapper()
  }

  @Test
  fun `test spec`() {
    val jsonString = Jsons.toPrettyString(encryptionMapper.spec().jsonSchema())
    // spot checking some values

    // making sure required fields are defined
    assertTrue(jsonString.contains("oneOf"))

    // oneOf for the encryption config definitions
    assertTrue(jsonString.contains("required"))

    // some algo names that should appear as constants
    assertTrue(jsonString.contains("AES"))
    assertTrue(jsonString.contains("RSA"))
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
            mode = "boom",
            padding = "none",
            key = "magic",
          ),
      )

    val testRecord = createRecord(nullTestFieldName, nullTestFieldName)
    encryptionMapper.map(config, testRecord)

    verifyRecordInvariant(testRecord).also {
      assertFalse(it.has(nullTestFieldName))
      assertTrue(
        it.asProtocol.record.meta.changes.contains(
          AirbyteRecordMessageMetaChange()
            .withField(nullTestFieldName)
            .withChange(AirbyteRecordMessageMetaChange.Change.NULLED)
            .withReason(AirbyteRecordMessageMetaChange.Reason.PLATFORM_SERIALIZATION_ERROR),
        ),
      )
    }
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
        mode = "CBC",
        padding = "PKCS5Padding",
        key = key.encoded.toHexString(),
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
        mode = "CBC",
        padding = "PKCS5Padding",
        key = key.encoded.toHexString(),
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

  private fun verifyRecordInvariant(record: AirbyteRecord): AirbyteRecord {
    println(record.asProtocol)
    assertEquals(SANITY_CHECK_VALUE, record.get(SANITY_CHECK_KEY).asString())
    return record
  }

  private fun createRecord(
    key: String,
    value: Any,
  ): AirbyteRecord =
    TestRecordAdapter(
      StreamDescriptor().withName("stream").withNamespace("ns"),
      mapOf(key to value, SANITY_CHECK_KEY to SANITY_CHECK_VALUE),
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