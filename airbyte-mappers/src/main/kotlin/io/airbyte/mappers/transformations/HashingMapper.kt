/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.security.MessageDigest
import java.util.HexFormat

@Singleton
@Named("HashingMapper")
class HashingMapper(
  private val objectMapper: ObjectMapper,
) : FilteredRecordsMapper<HashingMapperConfig>() {
  companion object {
    // Needed configuration keys
    const val TARGET_FIELD_CONFIG_KEY = "targetField"
    const val METHOD_CONFIG_KEY = "method"
    const val FIELD_NAME_SUFFIX_CONFIG_KEY = "fieldNameSuffix"

    // Supported hashing methods
    const val MD2 = "MD2"
    const val MD5 = "MD5"
    const val SHA1 = "SHA-1"
    const val SHA224 = "SHA-224"
    const val SHA256 = "SHA-256"
    const val SHA384 = "SHA-384"
    const val SHA512 = "SHA-512"

    val supportedMethods = listOf(MD2, MD5, SHA1, SHA224, SHA256, SHA384, SHA512)
  }

  private val hashingMapperSpec = HashingMapperSpec(objectMapper)

  override val name: String
    get() = MapperOperationName.HASHING

  override fun spec(): MapperSpec<HashingMapperConfig> = hashingMapperSpec

  override fun schema(
    config: HashingMapperConfig,
    slimStream: SlimStream,
  ): SlimStream {
    val resultField = "${config.config.targetField}${config.config.fieldNameSuffix}"

    return slimStream
      .deepCopy()
      .apply { redefineField(config.config.targetField, resultField, FieldType.STRING) }
  }

  override fun mapForNonDiscardedRecords(
    config: HashingMapperConfig,
    record: AirbyteRecord,
  ) {
    val outputFieldName = "${config.config.targetField}${config.config.fieldNameSuffix}"

    if (record.has(config.config.targetField)) {
      try {
        val data = record.get(config.config.targetField).asString().toByteArray()

        val hashedAndEncodeValue: String = hashAndEncodeData(config.config.method.value, data)
        record.set(outputFieldName, hashedAndEncodeValue)
      } catch (_: Exception) {
        // TODO We should use a more precise Reason once available in the protocol
        record.trackFieldError(outputFieldName, AirbyteRecord.Change.NULLED, AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR)
      } finally {
        record.remove(config.config.targetField)
      }
    }
  }

  private fun hashAndEncodeData(
    method: String,
    data: ByteArray,
  ): String {
    if (supportedMethods.contains(method).not()) {
      throw MapperException(type = DestinationCatalogGenerator.MapperErrorType.INVALID_MAPPER_CONFIG, message = "Unsupported hashing method: $method")
    }

    val hashedValue = MessageDigest.getInstance(method).digest(data)

    return HexFormat.of().formatHex(hashedValue)
  }
}
