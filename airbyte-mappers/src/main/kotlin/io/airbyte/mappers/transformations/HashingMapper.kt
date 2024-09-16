package io.airbyte.mappers.transformations

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName
import io.airbyte.config.MapperSpecification
import io.airbyte.config.MapperSpecificationFieldEnum
import io.airbyte.config.MapperSpecificationFieldString
import io.airbyte.config.adapters.AirbyteRecord
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.security.MessageDigest
import java.util.HexFormat

@Singleton
@Named("HashingMapper")
class HashingMapper : Mapper {
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

  override val name: String
    get() = MapperOperationName.HASHING

  override fun spec(): MapperSpecification {
    return MapperSpecification(
      name = name,
      documentationUrl = "",
      config =
        mapOf(
          TARGET_FIELD_CONFIG_KEY to
            MapperSpecificationFieldString(
              title = "Field",
              description = "The field to hash.",
            ),
          METHOD_CONFIG_KEY to
            MapperSpecificationFieldEnum(
              title = "Hashing method",
              description = "The hashing algorithm to use.",
              enum = supportedMethods,
              default = SHA256,
              examples = listOf(SHA256),
            ),
          FIELD_NAME_SUFFIX_CONFIG_KEY to
            MapperSpecificationFieldString(
              title = "Field name suffix",
              description = "The suffix to append to the field name after hashing.",
              default = "_hashed",
            ),
        ),
    )
  }

  override fun schema(
    config: ConfiguredMapper,
    slimStream: SlimStream,
  ): SlimStream {
    val (targetField, _, fieldNameSuffix) = getConfigValues(config.config)
    val resultField = "$targetField$fieldNameSuffix"

    return slimStream
      .deepCopy()
      .apply { redefineField(targetField, resultField, FieldType.STRING) }
  }

  override fun map(
    config: ConfiguredMapper,
    record: AirbyteRecord,
  ) {
    val (targetField, method, fieldNameSuffix) = getConfigValues(config.config)
    val outputFieldName = "$targetField$fieldNameSuffix"

    if (record.has(targetField)) {
      try {
        val data = record.get(targetField).asString().toByteArray()

        val hashedAndEncodeValue: String = hashAndEncodeData(method, data)
        record.set(outputFieldName, hashedAndEncodeValue)
      } catch (e: Exception) {
        // TODO We should use a more precise Reason once available in the protocol
        record.trackFieldError(outputFieldName, AirbyteRecord.Change.NULLED, AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR)
      } finally {
        record.remove(targetField)
      }
    }
  }

  internal fun hashAndEncodeData(
    method: String,
    data: ByteArray,
  ): String {
    if (supportedMethods.contains(method).not()) {
      throw IllegalArgumentException("Unsupported hashing method: $method")
    }

    val hashedValue = MessageDigest.getInstance(method).digest(data)

    return HexFormat.of().formatHex(hashedValue)
  }

  data class HashingConfig(
    val targetField: String,
    val method: String,
    val fieldNameSuffix: String,
  )

  private fun getConfigValues(config: Map<String, String>): HashingConfig {
    return HashingConfig(
      config[TARGET_FIELD_CONFIG_KEY] ?: "",
      config[METHOD_CONFIG_KEY] ?: "",
      config[FIELD_NAME_SUFFIX_CONFIG_KEY] ?: "_hashed",
    )
  }
}
