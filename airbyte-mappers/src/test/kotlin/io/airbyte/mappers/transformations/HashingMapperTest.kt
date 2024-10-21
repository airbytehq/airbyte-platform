package io.airbyte.mappers.transformations

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.adapters.TestRecordAdapter
import io.airbyte.mappers.transformations.HashingMapper.Companion.supportedMethods
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.security.Security

class HashingMapperTest {
  private val hashingMapper = spyk(HashingMapper())

  @Test
  fun specReturnsCorrectSpecification() {
    val spec = hashingMapper.spec()
    assertEquals("hashing", spec.name)
    assertEquals("", spec.documentationUrl)
    assertTrue(spec.config.containsKey(HashingMapper.TARGET_FIELD_CONFIG_KEY))
    assertTrue(spec.config.containsKey(HashingMapper.METHOD_CONFIG_KEY))
    assertTrue(spec.config.containsKey(HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY))
  }

  @Test
  fun schemaTransformsFieldNamesCorrectly() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1", HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed"))
    val slimStream = SlimStream(listOf(Field("field1", FieldType.DATE), Field("field2", FieldType.DATE)))
    val result = hashingMapper.schema(config, slimStream)
    assertEquals(listOf(Field("field1_hashed", FieldType.STRING), Field("field2", FieldType.DATE)), result.fields)
  }

  @Test
  fun schemaTransformsFieldNamesCorrectlyIfMissingSuffix() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1"))
    val slimStream = SlimStream(listOf(Field("field1", FieldType.DATE), Field("field2", FieldType.DATE)))
    val result = hashingMapper.schema(config, slimStream)
    assertEquals(listOf(Field("field1_hashed", FieldType.STRING), Field("field2", FieldType.DATE)), result.fields)
  }

  @Test
  fun schemaTransformsFailsIfFieldIsMissing() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1", HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed"))
    val slimStream = SlimStream(listOf(Field("NotField1", FieldType.STRING)))
    assertThrows(IllegalStateException::class.java) {
      hashingMapper.schema(config, slimStream)
    }
  }

  @Test
  fun schemaTransformFailsIfFieldCollision() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1", HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed"))
    val slimStream = SlimStream(listOf(Field("field1", FieldType.STRING), Field("field1_hashed", FieldType.STRING)))
    assertThrows(IllegalStateException::class.java) {
      hashingMapper.schema(config, slimStream)
    }
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      HashingMapper.MD2,
      HashingMapper.MD5,
      HashingMapper.SHA1,
      HashingMapper.SHA224,
      HashingMapper.SHA256,
      HashingMapper.SHA384,
      HashingMapper.SHA512,
    ],
  )
  fun mapHashesFieldCorrectly(hashingMethod: String) {
    val config =
      ConfiguredMapper(
        "test",
        mapOf(
          HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
          HashingMapper.METHOD_CONFIG_KEY to hashingMethod,
          HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
        ),
      )

    val record = TestRecordAdapter(StreamDescriptor().withName("stream"), mapOf("field1" to "value1", "field2" to "value2"))
    hashingMapper.map(config, record)

    assertTrue(record.has("field1_hashed"))
    val hashedValue = record.get("field1_hashed").asString()
    assertTrue("hashed_value" != hashedValue && hashedValue.all { it.isLetterOrDigit() }, "$hashedValue doesn't look like a valid hash")
    assertFalse(record.has("field1"))
    assertTrue(record.has("field2"))
    assertEquals("value2", record.get("field2").asString())
  }

  @Test
  fun mapHandlesUnsupportedHashingMethod() {
    val config =
      ConfiguredMapper(
        "test",
        mapOf(
          HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
          HashingMapper.METHOD_CONFIG_KEY to "unsupported",
          HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
        ),
      )

    val record = TestRecordAdapter(StreamDescriptor().withName("any"), mapOf("field1" to "anything"))
    hashingMapper.map(config, record)

    assertFalse(record.has("field1"))
    assertFalse(record.has("field1_hashed"))
    assertEquals(
      listOf(TestRecordAdapter.Change("field1_hashed", AirbyteRecord.Change.NULLED, AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR)),
      record.changes,
    )
  }

  @Test
  fun testAllMethodAreSupported() {
    val messageDigestAlgorithms = Security.getAlgorithms("MessageDigest")
    assertTrue(messageDigestAlgorithms.containsAll(supportedMethods))
  }
}
