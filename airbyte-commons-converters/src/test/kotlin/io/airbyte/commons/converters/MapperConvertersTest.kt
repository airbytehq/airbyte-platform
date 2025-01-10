package io.airbyte.commons.converters

import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.EncryptionMapperAESConfiguration
import io.airbyte.api.model.generated.EncryptionMapperAlgorithm
import io.airbyte.api.model.generated.EncryptionMapperRSAConfiguration
import io.airbyte.api.model.generated.FieldRenamingMapperConfiguration
import io.airbyte.api.model.generated.HashingMapperConfiguration
import io.airbyte.api.model.generated.HashingMapperConfiguration.MethodEnum
import io.airbyte.api.model.generated.RowFilteringMapperConfiguration
import io.airbyte.api.model.generated.RowFilteringOperation
import io.airbyte.api.model.generated.RowFilteringOperationType
import io.airbyte.api.model.generated.StreamMapperType
import io.airbyte.api.problems.throwable.generated.MapperValidationInvalidConfigProblem
import io.airbyte.api.problems.throwable.generated.MapperValidationMissingRequiredParamProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.AesMode
import io.airbyte.config.mapper.configs.AesPadding
import io.airbyte.config.mapper.configs.EncryptionMapperConfig
import io.airbyte.config.mapper.configs.EqualOperation
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
import java.util.UUID

class MapperConvertersTest {
  companion object {
    val MAPPER_ID: UUID = UUID.randomUUID()
  }

  @Test
  fun testConvertHashingMapper() {
    val hashingConfig =
      HashingMapperConfiguration()
        .method(MethodEnum.SHA_256)
        .fieldNameSuffix("_hash")
        .targetField("target_field")

    val configuredMapper =
      ConfiguredStreamMapper()
        .id(MAPPER_ID)
        .type(StreamMapperType.HASHING)
        .mapperConfiguration(Jsons.jsonNode(hashingConfig))

    val expectedConfig =
      HashingMapperConfig(
        id = MAPPER_ID,
        name = MapperOperationName.HASHING,
        config =
          HashingConfig(
            method = HashingMethods.SHA256,
            fieldNameSuffix = "_hash",
            targetField = "target_field",
          ),
      )

    val result = configuredMapper.toInternal()
    assertEquals(expectedConfig, result)

    val reverseResult = result.toApi()
    assertEquals(configuredMapper, reverseResult)
  }

  @Test
  fun testConvertFieldRenamingMapper() {
    val renamingConfig =
      FieldRenamingMapperConfiguration()
        .originalFieldName("old_field")
        .newFieldName("new_field")

    val configuredMapper =
      ConfiguredStreamMapper()
        .id(MAPPER_ID)
        .type(StreamMapperType.FIELD_RENAMING)
        .mapperConfiguration(Jsons.jsonNode(renamingConfig))

    val expectedConfig =
      FieldRenamingMapperConfig(
        id = MAPPER_ID,
        name = MapperOperationName.FIELD_RENAMING,
        config =
          FieldRenamingConfig(
            originalFieldName = "old_field",
            newFieldName = "new_field",
          ),
      )

    val result = configuredMapper.toInternal()
    assertEquals(expectedConfig, result)

    val reverseResult = result.toApi()
    assertEquals(configuredMapper, reverseResult)
  }

  @Test
  fun testAESEncryptionMapper() {
    val encryptionConfig =
      EncryptionMapperAESConfiguration()
        .algorithm(EncryptionMapperAlgorithm.AES)
        .fieldNameSuffix("enc")
        .targetField("target_field")
        .key("key")
        .mode(EncryptionMapperAESConfiguration.ModeEnum.CBC)
        .padding(EncryptionMapperAESConfiguration.PaddingEnum.PKCS5_PADDING)

    val configuredMapper =
      ConfiguredStreamMapper()
        .id(MAPPER_ID)
        .type(StreamMapperType.ENCRYPTION)
        .mapperConfiguration(Jsons.jsonNode(encryptionConfig))

    val expectedConfig =
      EncryptionMapperConfig(
        id = MAPPER_ID,
        name = MapperOperationName.ENCRYPTION,
        config =
          AesEncryptionConfig(
            algorithm = "AES",
            fieldNameSuffix = "enc",
            targetField = "target_field",
            key = AirbyteSecret.Hydrated("key"),
            mode = AesMode.CBC,
            padding = AesPadding.PKCS5Padding,
          ),
      )

    val result = configuredMapper.toInternal()
    assertEquals(expectedConfig, result)

    val reverseResult = result.toApi()
    assertEquals(configuredMapper, reverseResult)
  }

  @Test
  fun testRSAEncryptionMapper() {
    val encryptionConfig =
      EncryptionMapperRSAConfiguration()
        .algorithm(EncryptionMapperAlgorithm.RSA)
        .fieldNameSuffix("enc")
        .targetField("target_field")
        .publicKey("my_public_key")

    val configuredMapper =
      ConfiguredStreamMapper()
        .id(MAPPER_ID)
        .type(StreamMapperType.ENCRYPTION)
        .mapperConfiguration(Jsons.jsonNode(encryptionConfig))

    val expectedConfig =
      EncryptionMapperConfig(
        id = MAPPER_ID,
        name = MapperOperationName.ENCRYPTION,
        config =
          RsaEncryptionConfig(
            algorithm = "RSA",
            fieldNameSuffix = "enc",
            targetField = "target_field",
            publicKey = "my_public_key",
          ),
      )

    val result = configuredMapper.toInternal()
    assertEquals(expectedConfig, result)

    val reverseResult = result.toApi()
    assertEquals(configuredMapper, reverseResult)
  }

  @Test
  fun testRowFilteringMapper() {
    val rowFilteringConfig =
      RowFilteringMapperConfiguration()
        .conditions(
          RowFilteringOperation()
            .type(RowFilteringOperationType.NOT)
            .conditions(
              listOf(
                RowFilteringOperation()
                  .type(RowFilteringOperationType.EQUAL)
                  .fieldName("field1")
                  .comparisonValue("value1"),
              ),
            ),
        )

    val configuredMapper =
      ConfiguredStreamMapper()
        .id(MAPPER_ID)
        .type(StreamMapperType.ROW_FILTERING)
        .mapperConfiguration(Jsons.jsonNode(rowFilteringConfig))

    val expectedConfig =
      RowFilteringMapperConfig(
        id = MAPPER_ID,
        name = MapperOperationName.ROW_FILTERING,
        config =
          RowFilteringConfig(
            conditions =
              NotOperation(
                type = "NOT",
                conditions =
                  listOf(
                    EqualOperation(
                      type = "EQUAL",
                      fieldName = "field1",
                      comparisonValue = "value1",
                    ),
                  ),
              ),
          ),
      )

    val result = configuredMapper.toInternal()
    assertEquals(expectedConfig, result)

    val reverseResult = result.toApi()
    assertEquals(configuredMapper, reverseResult)
  }

  @Test
  fun testMissingRequiredFields() {
    val invalidJson = Jsons.jsonNode(mapOf("config_key" to "config_value"))
    assertThrows<MapperValidationMissingRequiredParamProblem> {
      ConfiguredStreamMapper().id(MAPPER_ID).type(StreamMapperType.HASHING).mapperConfiguration(invalidJson).toInternal()
    }
  }

  @Test
  fun testInvalidValues() {
    val invalidJson = Jsons.jsonNode(mapOf("method" to "some_invalid_method"))
    assertThrows<MapperValidationInvalidConfigProblem> {
      ConfiguredStreamMapper().id(MAPPER_ID).type(StreamMapperType.HASHING).mapperConfiguration(invalidJson).toInternal()
    }
  }
}
