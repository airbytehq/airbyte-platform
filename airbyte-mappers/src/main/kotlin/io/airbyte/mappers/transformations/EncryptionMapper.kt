package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.RsaEncryptionConfig
import jakarta.inject.Singleton
import java.security.KeyFactory
import java.security.SecureRandom
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@Singleton
class EncryptionMapper : FilteredRecordsMapper<EncryptionMapperConfig>() {
  class EncryptionMapperSpec : ConfigValidatingSpec<EncryptionMapperConfig>() {
    override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): EncryptionMapperConfig =
      Jsons.convertValue(configuredMapper, EncryptionMapperConfig::class.java)

    override fun jsonSchema(): JsonNode = simpleJsonSchemaGenerator.generateJsonSchema(specType())

    override fun specType(): Class<*> = EncryptionMapperConfig::class.java
  }

  override val name: String
    get() = MapperOperationName.ENCRYPTION

  override fun spec(): MapperSpec<EncryptionMapperConfig> = EncryptionMapperSpec()

  override fun schema(
    config: EncryptionMapperConfig,
    slimStream: SlimStream,
  ): SlimStream {
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
      } catch (e: Exception) {
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
  ): String {
    return when (config) {
      is AesEncryptionConfig -> encryptAES(data, config)
      is RsaEncryptionConfig -> encryptRSA(data, config)
    }
  }

  @OptIn(ExperimentalStdlibApi::class)
  private fun encryptAES(
    data: ByteArray,
    config: AesEncryptionConfig,
  ): String {
    val key = config.key as? AirbyteSecret.Hydrated ?: throw IllegalArgumentException("key hasn't been hydrated")
    val cipherName = "${config.algorithm}/${config.mode}/${config.padding}"
    val cipher = Cipher.getInstance(cipherName)
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
    val cipher = Cipher.getInstance(config.algorithm)
    val keyFactory = KeyFactory.getInstance("RSA")
    val keySpec = X509EncodedKeySpec(config.publicKey.hexToByteArray())
    cipher.init(Cipher.ENCRYPT_MODE, keyFactory.generatePublic(keySpec))
    return cipher.doFinal(data).toHexString()
  }
}
