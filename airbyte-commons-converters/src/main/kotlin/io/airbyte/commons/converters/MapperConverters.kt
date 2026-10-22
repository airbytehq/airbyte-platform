/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.exc.InvalidNullException
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.problems.model.generated.ProblemMapperIdData
import io.airbyte.api.problems.throwable.generated.MapperValidationInvalidConfigProblem
import io.airbyte.api.problems.throwable.generated.MapperValidationMissingRequiredParamProblem
import io.airbyte.api.server.generated.models.ConfiguredStreamMapper
import io.airbyte.api.server.generated.models.EncryptionMapperAESConfiguration.Mode
import io.airbyte.api.server.generated.models.EncryptionMapperAESConfiguration.Padding
import io.airbyte.api.server.generated.models.EncryptionMapperAlgorithm
import io.airbyte.api.server.generated.models.FieldFilteringMapperConfiguration
import io.airbyte.api.server.generated.models.FieldRenamingMapperConfiguration
import io.airbyte.api.server.generated.models.HashingMapperConfiguration
import io.airbyte.api.server.generated.models.RowFilteringOperationType
import io.airbyte.api.server.generated.models.StreamMapperType
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
import io.github.oshai.kotlinlogging.KotlinLogging

private val mapperLog = KotlinLogging.logger {}

// Mapper configurations travel as JSON. The generated models can't represent two of them: the
// RowFilteringOperation oneOf generates as Any, and the encryption configurations require a
// fieldNameSuffix that stored configurations may omit. These mirror the JSON the API has always
// sent and accepted.

@JsonInclude(JsonInclude.Include.NON_NULL)
private data class RowFilteringOperationJson(
  val type: RowFilteringOperationType? = null,
  val fieldName: String? = null,
  val comparisonValue: String? = null,
  val conditions: List<RowFilteringOperationJson> = emptyList(),
)

@JsonInclude(JsonInclude.Include.NON_NULL)
private data class RowFilteringMapperConfigurationJson(
  val conditions: RowFilteringOperationJson? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
private data class EncryptionMapperAESConfigurationJson(
  val algorithm: EncryptionMapperAlgorithm? = null,
  val fieldNameSuffix: String? = null,
  val key: String? = null,
  val mode: Mode? = null,
  val padding: Padding? = null,
  val targetField: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
private data class EncryptionMapperRSAConfigurationJson(
  val algorithm: EncryptionMapperAlgorithm? = null,
  val fieldNameSuffix: String? = null,
  val publicKey: String? = null,
  val targetField: String? = null,
)

private fun HashingMapperConfiguration.Method.toInternal(): HashingMethods =
  when (this) {
    HashingMapperConfiguration.Method.MD2 -> HashingMethods.MD2
    HashingMapperConfiguration.Method.MD5 -> HashingMethods.MD5
    HashingMapperConfiguration.Method.SHA_MINUS1 -> HashingMethods.SHA1
    HashingMapperConfiguration.Method.SHA_MINUS224 -> HashingMethods.SHA224
    HashingMapperConfiguration.Method.SHA_MINUS256 -> HashingMethods.SHA256
    HashingMapperConfiguration.Method.SHA_MINUS384 -> HashingMethods.SHA384
    HashingMapperConfiguration.Method.SHA_MINUS512 -> HashingMethods.SHA512
  }

private fun HashingMethods.toApi(): HashingMapperConfiguration.Method =
  when (this) {
    HashingMethods.MD2 -> HashingMapperConfiguration.Method.MD2
    HashingMethods.MD5 -> HashingMapperConfiguration.Method.MD5
    HashingMethods.SHA1 -> HashingMapperConfiguration.Method.SHA_MINUS1
    HashingMethods.SHA224 -> HashingMapperConfiguration.Method.SHA_MINUS224
    HashingMethods.SHA256 -> HashingMapperConfiguration.Method.SHA_MINUS256
    HashingMethods.SHA384 -> HashingMapperConfiguration.Method.SHA_MINUS384
    HashingMethods.SHA512 -> HashingMapperConfiguration.Method.SHA_MINUS512
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

private fun Mode.toInternal(): AesMode =
  when (this) {
    Mode.CBC -> AesMode.CBC
    Mode.CFB -> AesMode.CFB
    Mode.CTR -> AesMode.CTR
    Mode.ECB -> AesMode.ECB
    Mode.GCM -> AesMode.GCM
    Mode.OFB -> AesMode.OFB
  }

private fun Padding.toInternal(): AesPadding =
  when (this) {
    Padding.PKCS5_PADDING -> AesPadding.PKCS5Padding
    Padding.NO_PADDING -> AesPadding.NoPadding
  }

// a missing required field surfaces as a NullPointerException, reported as a missing parameter
private fun EncryptionMapperAESConfigurationJson.toInternal(): EncryptionConfig =
  AesEncryptionConfig(
    algorithm = ALGO_AES,
    key = AirbyteSecret.Hydrated(this.key!!),
    targetField = this.targetField!!,
    fieldNameSuffix = this.fieldNameSuffix,
    mode = this.mode!!.toInternal(),
    padding = this.padding!!.toInternal(),
  )

private fun EncryptionMapperRSAConfigurationJson.toInternal(): EncryptionConfig =
  RsaEncryptionConfig(
    algorithm = ALGO_RSA,
    publicKey = this.publicKey!!,
    targetField = this.targetField!!,
    fieldNameSuffix = this.fieldNameSuffix,
  )

private fun operationJsonToInternal(operationJson: ObjectNode): Operation =
  when (val type = operationJson.get("type")!!.textValue()) {
    RowFilteringOperationType.EQUAL.toString() -> {
      val apiOperation = Jsons.convertValue(operationJson, RowFilteringOperationJson::class.java)
      EqualOperation(
        type = "EQUAL",
        fieldName = apiOperation.fieldName!!,
        comparisonValue = apiOperation.comparisonValue!!,
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
    when (this.type) {
      StreamMapperType.HASHING -> {
        HashingMapperConfig(
          id = this.id,
          name = MapperOperationName.HASHING,
          config = Jsons.convertValue(this.mapperConfiguration, HashingMapperConfiguration::class.java).toInternal(),
        )
      }

      StreamMapperType.FIELD_MINUS_FILTERING -> {
        FieldFilteringMapperConfig(
          id = this.id,
          name = MapperOperationName.FIELD_FILTERING,
          config = Jsons.convertValue(this.mapperConfiguration, FieldFilteringMapperConfiguration::class.java).toInternal(),
        )
      }

      StreamMapperType.FIELD_MINUS_RENAMING -> {
        FieldRenamingMapperConfig(
          id = this.id,
          name = MapperOperationName.FIELD_RENAMING,
          config = Jsons.convertValue(this.mapperConfiguration, FieldRenamingMapperConfiguration::class.java).toInternal(),
        )
      }

      StreamMapperType.ROW_MINUS_FILTERING -> {
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
              Jsons.convertValue(this.mapperConfiguration, EncryptionMapperAESConfigurationJson::class.java).toInternal()
            }

            EncryptionMapperAlgorithm.RSA.toString() -> {
              Jsons.convertValue(this.mapperConfiguration, EncryptionMapperRSAConfigurationJson::class.java).toInternal()
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
    // a generated model rejects a missing required field while it's built, rather than with a null check later
    if (e.cause is InvalidNullException) {
      mapperLog.error(e) { "Missing required configuration for mapper ${this.id}" }
      throw MapperValidationMissingRequiredParamProblem(
        ProblemMapperIdData()
          .mapperId(this.id)
          .mapperType(this.type.toString()),
      )
    }
    mapperLog.error(e) { "Invalid mapper configuration for mapper ${this.id}" }
    throw MapperValidationInvalidConfigProblem(
      ProblemMapperIdData()
        .mapperId(this.id)
        .mapperType(this.type.toString()),
    )
  } catch (e: NullPointerException) {
    mapperLog.error(e) { "Missing required configuration for mapper ${this.id}" }
    throw MapperValidationMissingRequiredParamProblem(
      ProblemMapperIdData()
        .mapperId(this.id)
        .mapperType(this.type.toString()),
    )
  }

private fun HashingMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper(
    id = this.id,
    type = StreamMapperType.HASHING,
    mapperConfiguration =
      Jsons.jsonNode(
        HashingMapperConfiguration(
          method = this.config.method.toApi(),
          fieldNameSuffix = this.config.fieldNameSuffix,
          targetField = this.config.targetField,
        ),
      ),
  )

private fun FieldFilteringMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper(
    id = this.id,
    type = StreamMapperType.FIELD_MINUS_FILTERING,
    mapperConfiguration =
      Jsons.jsonNode(
        FieldFilteringMapperConfiguration(targetField = this.config.targetField),
      ),
  )

private fun FieldRenamingMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper(
    id = this.id,
    type = StreamMapperType.FIELD_MINUS_RENAMING,
    mapperConfiguration =
      Jsons.jsonNode(
        FieldRenamingMapperConfiguration(
          originalFieldName = this.config.originalFieldName,
          newFieldName = this.config.newFieldName,
        ),
      ),
  )

private fun Operation.toApi(): RowFilteringOperationJson =
  when (this) {
    is EqualOperation ->
      RowFilteringOperationJson(
        type = RowFilteringOperationType.EQUAL,
        fieldName = this.fieldName,
        comparisonValue = this.comparisonValue,
      )
    is NotOperation ->
      RowFilteringOperationJson(
        type = RowFilteringOperationType.NOT,
        conditions = this.conditions.map { it.toApi() },
      )
    else -> throw IllegalArgumentException("Unsupported operation type: ${this.type}")
  }

private fun RowFilteringMapperConfig.toApi(): ConfiguredStreamMapper =
  ConfiguredStreamMapper(
    id = this.id,
    type = StreamMapperType.ROW_MINUS_FILTERING,
    mapperConfiguration =
      Jsons.jsonNode(
        RowFilteringMapperConfigurationJson(conditions = this.config.conditions.toApi()),
      ),
  )

private fun AesMode.toApi(): Mode =
  when (this) {
    AesMode.CBC -> Mode.CBC
    AesMode.CFB -> Mode.CFB
    AesMode.CTR -> Mode.CTR
    AesMode.ECB -> Mode.ECB
    AesMode.GCM -> Mode.GCM
    AesMode.OFB -> Mode.OFB
  }

private fun AesPadding.toApi(): Padding =
  when (this) {
    AesPadding.PKCS5Padding -> Padding.PKCS5_PADDING
    AesPadding.NoPadding -> Padding.NO_PADDING
  }

private fun AirbyteSecret.toApi(): String =
  when (this) {
    is AirbyteSecret.Hydrated -> this.value
    is AirbyteSecret.Reference -> AirbyteSecretConstants.SECRETS_MASK
  }

private fun AesEncryptionConfig.toApi(): EncryptionMapperAESConfigurationJson =
  EncryptionMapperAESConfigurationJson(
    algorithm = EncryptionMapperAlgorithm.AES,
    key = this.key.toApi(),
    targetField = this.targetField,
    fieldNameSuffix = this.fieldNameSuffix,
    mode = this.mode.toApi(),
    padding = this.padding.toApi(),
  )

private fun RsaEncryptionConfig.toApi(): EncryptionMapperRSAConfigurationJson =
  EncryptionMapperRSAConfigurationJson(
    algorithm = EncryptionMapperAlgorithm.RSA,
    publicKey = this.publicKey,
    targetField = this.targetField,
    fieldNameSuffix = this.fieldNameSuffix,
  )

private fun EncryptionMapperConfig.toApi(): ConfiguredStreamMapper {
  val config =
    when (this.config) {
      is AesEncryptionConfig -> Jsons.jsonNode((this.config as AesEncryptionConfig).toApi())
      is RsaEncryptionConfig -> Jsons.jsonNode((this.config as RsaEncryptionConfig).toApi())
    }
  return ConfiguredStreamMapper(
    id = this.id,
    type = StreamMapperType.ENCRYPTION,
    mapperConfiguration = config,
  )
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
