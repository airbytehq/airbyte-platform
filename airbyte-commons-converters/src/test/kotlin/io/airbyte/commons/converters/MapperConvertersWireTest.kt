/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.api.problems.throwable.generated.MapperValidationInvalidConfigProblem
import io.airbyte.api.problems.throwable.generated.MapperValidationMissingRequiredParamProblem
import io.airbyte.api.server.generated.models.ConfiguredStreamMapper
import io.airbyte.api.server.generated.models.StreamMapperType
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.AesMode
import io.airbyte.config.mapper.configs.AesPadding
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.EqualOperation
import io.airbyte.config.mapper.configs.FieldFilteringConfig
import io.airbyte.config.mapper.configs.FieldFilteringMapperConfig
import io.airbyte.config.mapper.configs.FieldRenamingConfig
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.HashingMethods
import io.airbyte.config.mapper.configs.NotOperation
import io.airbyte.config.mapper.configs.RowFilteringConfig
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import io.airbyte.config.mapper.configs.RsaEncryptionConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

/**
 * Pins the mapper configuration JSON the API sends and accepts. The expected documents are what the
 * Java-generated models produced, so the Kotlin models must keep them byte-for-byte.
 */
class MapperConvertersWireTest {
  @ParameterizedTest(name = "{0}")
  @MethodSource("mappers")
  fun `toApi sends the mapper configuration JSON`(
    name: String,
    internal: MapperConfig,
    expectedJson: String,
  ) {
    assertEquals(Jsons.deserialize(expectedJson), internal.toApi().mapperConfiguration)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mappers")
  fun `toInternal reads the mapper configuration JSON`(
    name: String,
    internal: MapperConfig,
    json: String,
  ) {
    val apiMapper = ConfiguredStreamMapper(id = MAPPER_ID, type = internal.toApi().type, mapperConfiguration = Jsons.deserialize(json))
    assertEquals(internal.toApi(), apiMapper.toInternal().toApi())
  }

  @Test
  fun `a missing required field is reported as a missing parameter`() {
    val mapper =
      ConfiguredStreamMapper(
        id = MAPPER_ID,
        type = StreamMapperType.HASHING,
        mapperConfiguration = Jsons.deserialize("""{"method":"SHA-256","fieldNameSuffix":"_hash"}"""),
      )
    assertThrows<MapperValidationMissingRequiredParamProblem> { mapper.toInternal() }
  }

  @Test
  fun `a missing required encryption field is reported as a missing parameter`() {
    val mapper =
      ConfiguredStreamMapper(
        id = MAPPER_ID,
        type = StreamMapperType.ENCRYPTION,
        mapperConfiguration = Jsons.deserialize("""{"algorithm":"RSA","targetField":"f","fieldNameSuffix":"_enc"}"""),
      )
    assertThrows<MapperValidationMissingRequiredParamProblem> { mapper.toInternal() }
  }

  @Test
  fun `an unknown enum value is reported as an invalid configuration`() {
    val mapper =
      ConfiguredStreamMapper(
        id = MAPPER_ID,
        type = StreamMapperType.HASHING,
        mapperConfiguration = Jsons.deserialize("""{"method":"NOT-A-METHOD","fieldNameSuffix":"_hash","targetField":"f"}"""),
      )
    assertThrows<MapperValidationInvalidConfigProblem> { mapper.toInternal() }
  }

  companion object {
    private val MAPPER_ID: UUID = UUID.fromString("5c1c7a9e-6b3e-4c0a-9d2a-0b1e2f3a4b5c")

    @JvmStatic
    fun mappers(): List<Arguments> =
      listOf(
        Arguments.of(
          "hashing",
          HashingMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.HASHING,
            config = HashingConfig(method = HashingMethods.SHA256, fieldNameSuffix = "_hash", targetField = "email"),
          ),
          """{"fieldNameSuffix":"_hash","method":"SHA-256","targetField":"email"}""",
        ),
        Arguments.of(
          "field filtering",
          FieldFilteringMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.FIELD_FILTERING,
            config = FieldFilteringConfig(targetField = "secret"),
          ),
          """{"targetField":"secret"}""",
        ),
        Arguments.of(
          "field renaming",
          FieldRenamingMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.FIELD_RENAMING,
            config = FieldRenamingConfig(originalFieldName = "old", newFieldName = "new"),
          ),
          """{"newFieldName":"new","originalFieldName":"old"}""",
        ),
        Arguments.of(
          "row filtering equal",
          RowFilteringMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.ROW_FILTERING,
            config = RowFilteringConfig(conditions = EqualOperation(type = "EQUAL", fieldName = "status", comparisonValue = "active")),
          ),
          """{"conditions":{"comparisonValue":"active","fieldName":"status","type":"EQUAL","conditions":[]}}""",
        ),
        Arguments.of(
          "row filtering not",
          RowFilteringMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.ROW_FILTERING,
            config =
              RowFilteringConfig(
                conditions =
                  NotOperation(
                    type = "NOT",
                    conditions = listOf(EqualOperation(type = "EQUAL", fieldName = "status", comparisonValue = "deleted")),
                  ),
              ),
          ),
          """{"conditions":{"type":"NOT","conditions":[{"comparisonValue":"deleted","fieldName":"status","type":"EQUAL","conditions":[]}]}}""",
        ),
        Arguments.of(
          "aes encryption",
          EncryptionMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.ENCRYPTION,
            config =
              AesEncryptionConfig(
                algorithm = EncryptionConfig.ALGO_AES,
                key = AirbyteSecret.Hydrated("k"),
                targetField = "ssn",
                fieldNameSuffix = "_enc",
                mode = AesMode.GCM,
                padding = AesPadding.NoPadding,
              ),
          ),
          """{"algorithm":"AES","fieldNameSuffix":"_enc","key":"k","mode":"GCM","padding":"NoPadding","targetField":"ssn"}""",
        ),
        Arguments.of(
          "rsa encryption",
          EncryptionMapperConfig(
            id = MAPPER_ID,
            name = MapperOperationName.ENCRYPTION,
            config = RsaEncryptionConfig(algorithm = EncryptionConfig.ALGO_RSA, publicKey = "pk", targetField = "ssn", fieldNameSuffix = "_enc"),
          ),
          """{"algorithm":"RSA","fieldNameSuffix":"_enc","publicKey":"pk","targetField":"ssn"}""",
        ),
      )
  }
}
