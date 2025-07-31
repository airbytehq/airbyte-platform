/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.EncryptionMapperAESConfiguration
import io.airbyte.api.model.generated.EncryptionMapperAlgorithm
import io.airbyte.api.model.generated.EncryptionMapperRSAConfiguration
import io.airbyte.api.model.generated.FieldFilteringMapperConfiguration
import io.airbyte.api.model.generated.FieldRenamingMapperConfiguration
import io.airbyte.api.model.generated.HashingMapperConfiguration
import io.airbyte.api.model.generated.HashingMapperConfiguration.MethodEnum
import io.airbyte.api.model.generated.RowFilteringMapperConfiguration
import io.airbyte.api.model.generated.RowFilteringOperation
import io.airbyte.api.model.generated.RowFilteringOperationEqual
import io.airbyte.api.model.generated.RowFilteringOperationType
import io.airbyte.api.model.generated.StreamMapperType
import io.airbyte.api.problems.model.generated.ProblemMapperIdData
import io.airbyte.api.problems.throwable.generated.MapperValidationInvalidConfigProblem
import io.airbyte.api.problems.throwable.generated.MapperValidationMissingRequiredParamProblem
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.AesEncryptionConfig
import io.airbyte.config.mapper.configs.AesMode
import io.airbyte.config.mapper.configs.AesPadding
import io.airbyte.config.mapper.configs.EncryptionConfig
import io.airbyte.config.mapper.configs.EncryptionConfig.Companion.ALGO_AES
import io.airbyte.config.mapper.configs.EncryptionConfig.Companion.ALGO_RSA
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
import io.airbyte.config.mapper.configs.Operation
import io.airbyte.config.mapper.configs.RowFilteringConfig
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import io.airbyte.config.mapper.configs.RsaEncryptionConfig

private fun MethodEnum.toInternal(): HashingMethods =
  when (this) {
    MethodEnum.MD2 -> HashingMethods.MD2
    MethodEnum.MD5 -> HashingMethods.MD5
    MethodEnum.SHA_1 -> HashingMethods.SHA1
    MethodEnum.SHA_224 -> HashingMethods.SHA224
    MethodEnum.SHA_256 -> HashingMethods.SHA256
    MethodEnum.SHA_384 -> HashingMethods.SHA384
    MethodEnum.SHA_512 -> HashingMethods.SHA512
  }

private fun HashingMethods.toApi(): MethodEnum =
  when (this) {
    HashingMethods.MD2 -> MethodEnum.MD2
    HashingMethods.MD5 -> MethodEnum.MD5
    HashingMethods.SHA1 -> MethodEnum.SHA_1
    HashingMethods.SHA224 -> MethodEnum.SHA_224
    HashingMethods.SHA256 -> MethodEnum.SHA_256
    HashingMethods.SHA384 -> MethodEnum.SHA_384
    HashingMethods.SHA512 -> MethodEnum.SHA_512
  }

private fun HashingMapperConfiguration.toInternal(): HashingConfig =
  HashingConfig(
    method = this.method.toInternal(),
    fieldNameSuffix = this.fieldNameSuffix,
    targetField = this.targetField,
  )

private fun FieldFilteringMapperConfiguration.toInternal(): FieldFilteringConfig =
  FieldFilteringConfig(
    targetField = this.targetField,
  )

private fun FieldRenamingMapperConfiguration.toInternal(): FieldRenamingConfig =
  FieldRenamingConfig(
    originalFieldName = this.originalFieldName,
    newFieldName = this.newFieldName,
  )

private fun EncryptionMapperAESConfiguration.ModeEnum.toInternal(): AesMode =
  when (this) {
    EncryptionMapperAESConfiguration.ModeEnum.CBC -> AesMode.CBC
    EncryptionMapperAESConfiguration.ModeEnum.CFB -> AesMode.CFB
    EncryptionMapperAESConfiguration.ModeEnum.CTR -> AesMode.CTR
    EncryptionMapperAESConfiguration.ModeEnum.ECB -> AesMode.ECB
    EncryptionMapperAESConfiguration.ModeEnum.GCM -> AesMode.GCM
    EncryptionMapperAESConfiguration.ModeEnum.OFB -> AesMode.OFB
  }

private fun EncryptionMapperAESConfiguration.PaddingEnum.toInternal(): AesPadding =
  when (this) {
    EncryptionMapperAESConfiguration.PaddingEnum.PKCS5_PADDING -> AesPadding.PKCS5Padding
    EncryptionMapperAESConfiguration.PaddingEnum.NO_PADDING -> AesPadding.NoPadding
  }

private fun EncryptionMapperAESConfiguration.toInternal(): EncryptionConfig =
  AesEncryptionConfig(
    algorithm = ALGO_AES,
    key = AirbyteSecret.Hydrated(this.key),
    targetField = this.targetField,
    fieldNameSuffix = this.fieldNameSuffix,
    mode = this.mode.toInternal(),
    padding = this.padding.toInternal(),
  )

private fun EncryptionMapperRSAConfiguration.toInternal(): EncryptionConfig =
  RsaEncryptionConfig(
    algorithm = ALGO_RSA,
    publicKey = this.publicKey,
    targetField = this.targetField,
    fieldNameSuffix = this.fieldNameSuffix,
  )

private fun operationJsonToInternal(operationJson: ObjectNode): Operation =
  when (val type = operationJson.get("type")!!.textValue()) {
    RowFilteringOperationType.EQUAL.toString() -> {
      val apiOperation = Jsons.convertValue(operationJson, RowFilteringOperationEqual::class.java)
      EqualOperation(
        type = "EQUAL",
        fieldName = apiOperation.fieldName,
        comparisonValue = apiOperation.comparisonValue,
      )
    }
    RowFilteringOperationType.NOT.toString() -> {
      val conditions = nestedConditionsToInternal(operationJson)
      NotOperation(
        type = "NOT",
        conditions = conditions,
      )
    }
    else -> throw IllegalArgumentException("Unsupported operation type: $type")
  }

private fun nestedConditionsToInternal(operation: ObjectNode): List<Operation> =
  operation.get("conditions")!!.toList().map {
    operationJsonToInternal(it as ObjectNode)
  }

fun ConfiguredStreamMapper.toInternal(): MapperConfig =
  try {
    when (this.type!!) {
      StreamMapperType.HASHING -> {
        HashingMapperConfig(
          id = this.id,
          name = MapperOperationName.HASHING,
          config = Jsons.convertValue(this.mapperConfiguration, HashingMapperConfiguration::class.java).toInternal(),
        )
      }

      StreamMapperType.FIELD_FILTERING -> {
        FieldFilteringMapperConfig(
          id = this.id,
          name = MapperOperationName.FIELD_FILTERING,
          config = Jsons.convertValue(this.mapperConfiguration, FieldFilteringMapperConfiguration::class.java).toInternal(),
        )
      }

      StreamMapperType.FIELD_RENAMING -> {
        FieldRenamingMapperConfig(
          id = this.id,
          name = MapperOperationName.FIELD_RENAMING,
          config = Jsons.convertValue(this.mapperConfiguration, FieldRenamingMapperConfiguration::class.java).toInternal(),
        )
      }

      StreamMapperType.ROW_FILTERING -> {
        val initialOperation = this.mapperConfiguration.get("conditions") as ObjectNode
        val operationConfig = operationJsonToInternal(initialOperation)
        RowFilteringMapperConfig(
          id = this.id,
          name = MapperOperationName.ROW_FILTERING,
          config = RowFilteringConfig(conditions = operationConfig),
        )
      }

      StreamMapperType.ENCRYPTION -> {
        val config =
          when (val encAlgo = this.mapperConfiguration.get("algorithm")!!.textValue()) {
            EncryptionMapperAlgorithm.AES.toString() -> {
              Jsons.convertValue(this.mapperConfiguration, EncryptionMapperAESConfiguration::class.java).toInternal()
            }

            EncryptionMapperAlgorithm.RSA.toString() -> {
              Jsons.convertValue(this.mapperConfiguration, EncryptionMapperRSAConfiguration::class.java).toInternal()
            }

            else -> throw IllegalArgumentException("Unsupported encryption algorithm: $encAlgo")
          }

        EncryptionMapperConfig(
          id = this.id,
          name = MapperOperationName.ENCRYPTION,
          config = config,
        )
      }
    }
  } catch (e: IllegalArgumentException) {
    throw MapperValidationInvalidConfigProblem(
      ProblemMapperIdData()
        .mapperId(this.id)
        .mapperType(this.type.toString())
        .message(e.message),
    )
  } catch (e: NullPointerException) {
    throw MapperValidationMissingRequiredParamProblem(
      ProblemMapperIdData()
        .mapperId(this.id)
        .mapperType(this.type.toString())
        .message(e.message),
    )
  }

private fun HashingMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper()
    .id(this.id)
    .type(StreamMapperType.HASHING)
    .mapperConfiguration(
      Jsons.jsonNode(
        HashingMapperConfiguration()
          .method(this.config.method.toApi())
          .fieldNameSuffix(this.config.fieldNameSuffix)
          .targetField(this.config.targetField),
      ),
    )

private fun FieldFilteringMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper()
    .id(this.id)
    .type(StreamMapperType.FIELD_FILTERING)
    .mapperConfiguration(
      Jsons.jsonNode(
        FieldFilteringMapperConfiguration().targetField(this.config.targetField),
      ),
    )

private fun FieldRenamingMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper()
    .id(this.id)
    .type(StreamMapperType.FIELD_RENAMING)
    .mapperConfiguration(
      Jsons.jsonNode(
        FieldRenamingMapperConfiguration()
          .originalFieldName(this.config.originalFieldName)
          .newFieldName(this.config.newFieldName),
      ),
    )

private fun Operation.toApi(): RowFilteringOperation =
  when (this) {
    is EqualOperation ->
      RowFilteringOperation()
        .type(RowFilteringOperationType.EQUAL)
        .fieldName(this.fieldName)
        .comparisonValue(this.comparisonValue)
    is NotOperation ->
      RowFilteringOperation()
        .type(RowFilteringOperationType.NOT)
        .conditions(this.conditions.map { it.toApi() })
    else -> throw IllegalArgumentException("Unsupported operation type: ${this.type}")
  }

private fun RowFilteringMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper()
    .id(this.id)
    .type(StreamMapperType.ROW_FILTERING)
    .mapperConfiguration(
      Jsons.jsonNode(
        RowFilteringMapperConfiguration()
          .conditions(this.config.conditions.toApi()),
      ),
    )

private fun AesMode.toApi(): EncryptionMapperAESConfiguration.ModeEnum =
  when (this) {
    AesMode.CBC -> EncryptionMapperAESConfiguration.ModeEnum.CBC
    AesMode.CFB -> EncryptionMapperAESConfiguration.ModeEnum.CFB
    AesMode.CTR -> EncryptionMapperAESConfiguration.ModeEnum.CTR
    AesMode.ECB -> EncryptionMapperAESConfiguration.ModeEnum.ECB
    AesMode.GCM -> EncryptionMapperAESConfiguration.ModeEnum.GCM
    AesMode.OFB -> EncryptionMapperAESConfiguration.ModeEnum.OFB
  }

private fun AesPadding.toApi(): EncryptionMapperAESConfiguration.PaddingEnum =
  when (this) {
    AesPadding.PKCS5Padding -> EncryptionMapperAESConfiguration.PaddingEnum.PKCS5_PADDING
    AesPadding.NoPadding -> EncryptionMapperAESConfiguration.PaddingEnum.NO_PADDING
  }

private fun AirbyteSecret.toApi(): String =
  when (this) {
    is AirbyteSecret.Hydrated -> this.value
    is AirbyteSecret.Reference -> AirbyteSecretConstants.SECRETS_MASK
  }

private fun AesEncryptionConfig.toApi(): EncryptionMapperAESConfiguration =
  EncryptionMapperAESConfiguration()
    .algorithm(EncryptionMapperAlgorithm.AES)
    .key(this.key.toApi())
    .targetField(this.targetField)
    .fieldNameSuffix(this.fieldNameSuffix)
    .mode(this.mode.toApi())
    .padding(this.padding.toApi())

private fun RsaEncryptionConfig.toApi(): EncryptionMapperRSAConfiguration =
  EncryptionMapperRSAConfiguration()
    .algorithm(EncryptionMapperAlgorithm.RSA)
    .publicKey(this.publicKey)
    .targetField(this.targetField)
    .fieldNameSuffix(this.fieldNameSuffix)

private fun EncryptionMapperConfig.toApi(): ConfiguredStreamMapper {
  val config =
    when (this.config) {
      is AesEncryptionConfig -> Jsons.jsonNode((this.config as AesEncryptionConfig).toApi())
      is RsaEncryptionConfig -> Jsons.jsonNode((this.config as RsaEncryptionConfig).toApi())
    }
  return ConfiguredStreamMapper()
    .id(this.id)
    .type(StreamMapperType.ENCRYPTION)
    .mapperConfiguration(config)
}

fun MapperConfig.toApi(): ConfiguredStreamMapper =
  when (this) {
    is HashingMapperConfig -> this.toApi()
    is FieldFilteringMapperConfig -> this.toApi()
    is FieldRenamingMapperConfig -> this.toApi()
    is RowFilteringMapperConfig -> this.toApi()
    is EncryptionMapperConfig -> this.toApi()
    else -> throw IllegalArgumentException("Mapper type is not supported")
  }
