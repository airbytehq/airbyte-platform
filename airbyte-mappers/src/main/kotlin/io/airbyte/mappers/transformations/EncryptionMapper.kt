/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.RsaEncryptionConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import jakarta.inject.Singleton
import java.security.KeyFactory
import java.security.SecureRandom
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

class EncryptionConfigException(
  msg: String,
  cause: Throwable,
) : MapperException(
    type = DestinationCatalogGenerator.MapperErrorType.INVALID_MAPPER_CONFIG,
    message = msg,
    cause = cause,
  )

class MissingSecretValueException(
  msg: String,
) : IllegalArgumentException(msg)

@Singleton
class EncryptionMapper(
  private val objectMapper: ObjectMapper,
) : FilteredRecordsMapper<EncryptionMapperConfig>() {
  class EncryptionMapperSpec(
    objectMapper: ObjectMapper,
  ) : ConfigValidatingSpec<EncryptionMapperConfig>(objectMapper) {
    override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): EncryptionMapperConfig =
      objectMapper().convertValue(configuredMapper, EncryptionMapperConfig::class.java)

    override fun jsonSchema(): JsonNode = simpleJsonSchemaGenerator.generateJsonSchema(specType())

    override fun specType(): Class<*> = EncryptionMapperConfig::class.java
  }

  override val name: String
    get() = MapperOperationName.ENCRYPTION

  override fun spec(): MapperSpec<EncryptionMapperConfig> = EncryptionMapperSpec(objectMapper = objectMapper)

  override fun schema(
    config: EncryptionMapperConfig,
    slimStream: SlimStream,
  ): SlimStream {
    // Making sure we can instantiate cipher for the given config.
    getCipher(config.config)

    // Try to encrypt to ensure params are valid
    encryptSample(config.config)

    return slimStream
      .deepCopy()
      .apply { redefineField(config.config.targetField, getOutputFieldName(config), FieldType.STRING) }
  }

  override fun mapForNonDiscardedRecords(
    config: EncryptionMapperConfig,
    record: AirbyteRecord,
  ) {
    if (record.has(config.config.targetField)) {
      val outputFieldName = getOutputFieldName(config)
      var failed = false
      try {
        val data = record.get(config.config.targetField).asString()
        val encryptedData = encrypt(data.toByteArray(Charsets.UTF_8), config.config)
        record.set(outputFieldName, encryptedData)
      } catch (_: Exception) {
        // TODO We should use a more precise Reason once available in the protocol
        record.trackFieldError(outputFieldName, AirbyteRecord.Change.NULLED, AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR)
        failed = true
      } finally {
        if (failed || outputFieldName != config.config.targetField) {
          record.remove(config.config.targetField)
        }
      }
    }
  }

  private fun getOutputFieldName(config: EncryptionMapperConfig): String = "${config.config.targetField}${config.config.fieldNameSuffix ?: ""}"

  private fun encrypt(
    data: ByteArray,
    config: EncryptionConfig,
  ): String =
    when (config) {
      is AesEncryptionConfig -> encryptAES(data, config)
      is RsaEncryptionConfig -> encryptRSA(data, config)
    }

  private fun encryptSample(config: EncryptionConfig) {
    val sampleData = "sample data"
    try {
      encrypt(sampleData.toByteArray(Charsets.UTF_8), config)
    } catch (_: MissingSecretValueException) {
      // ignore if key is not hydrated
    } catch (e: Exception) {
      throw EncryptionConfigException("Encryption key or parameters are invalid", e)
    }
  }

  private fun getCipher(config: EncryptionConfig): Cipher =
    when (config) {
      is AesEncryptionConfig ->
        try {
          Cipher.getInstance("${config.algorithm}/${config.mode}/${config.padding}")
        } catch (e: Exception) {
          throw EncryptionConfigException("Mode ${config.mode} and padding ${config.padding} are incompatible for AES Encryption", e)
        }
      is RsaEncryptionConfig -> Cipher.getInstance(config.algorithm)
    }

  @OptIn(ExperimentalStdlibApi::class)
  private fun encryptAES(
    data: ByteArray,
    config: AesEncryptionConfig,
  ): String {
    val key = config.key as? AirbyteSecret.Hydrated ?: throw MissingSecretValueException("key hasn't been hydrated")
    val cipher = getCipher(config)
    val iv = ByteArray(16)
    SecureRandom().nextBytes(iv)
    cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(key.value.hexToByteArray(), config.algorithm), IvParameterSpec(iv))
    val encryptedData = cipher.doFinal(data)
    return (iv + encryptedData).toHexString()
  }

  @OptIn(ExperimentalStdlibApi::class)
  private fun encryptRSA(
    data: ByteArray,
    config: RsaEncryptionConfig,
  ): String {
    val cipher = getCipher(config)
    val keyFactory = KeyFactory.getInstance(config.algorithm)
    val keySpec = X509EncodedKeySpec(config.publicKey.hexToByteArray())
    cipher.init(Cipher.ENCRYPT_MODE, keyFactory.generatePublic(keySpec))
    return cipher.doFinal(data).toHexString()
  }
}
