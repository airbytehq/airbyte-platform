/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.mapper.configs

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.airbyte.config.AirbyteSecret
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import java.util.UUID

data class EncryptionMapperConfig(
  @JsonProperty("name")
  @field:NotNull
  @field:SchemaDescription("The name of the operation.")
  @field:SchemaConstant(MapperOperationName.ENCRYPTION)
  val name: String = MapperOperationName.ENCRYPTION,
  @JsonIgnore
  @field:SchemaDescription("URL for documentation related to this configuration.")
  @field:SchemaFormat("uri")
  val documentationUrl: String? = null,
  @JsonProperty("config")
  @field:NotNull
  val config: EncryptionConfig,
  val id: UUID? = null,
) : MapperConfig {
  override fun name(): String = name

  override fun documentationUrl(): String? = documentationUrl

  override fun id(): UUID? = id

  override fun config(): Any = config
}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "algorithm",
  visible = true,
)
@JsonSubTypes(
  JsonSubTypes.Type(value = AesEncryptionConfig::class, name = EncryptionConfig.ALGO_AES),
  JsonSubTypes.Type(value = RsaEncryptionConfig::class, name = EncryptionConfig.ALGO_RSA),
)
sealed class EncryptionConfig {
  companion object {
    const val ALGO_AES = "AES"
    const val ALGO_RSA = "RSA"
  }

  abstract val algorithm: String
  abstract val targetField: String
  abstract val fieldNameSuffix: String?
}

enum class AesMode {
  CBC,
  CFB,
  OFB,
  CTR,
  GCM,
  ECB,
}

enum class AesPadding {
  NoPadding,
  PKCS5Padding,
}

data class AesEncryptionConfig(
  @JsonProperty("algorithm")
  @field:NotNull
  @field:SchemaConstant(ALGO_AES)
  override val algorithm: String,
  @JsonProperty("targetField")
  @field:NotNull
  override val targetField: String,
  @JsonProperty("fieldNameSuffix")
  override val fieldNameSuffix: String? = null,
  @JsonProperty("mode")
  @field:NotNull
  val mode: AesMode,
  @JsonProperty("padding")
  @field:NotNull
  val padding: AesPadding,
  @JsonProperty("key")
  @field:NotNull
  val key: AirbyteSecret,
) : EncryptionConfig()

data class RsaEncryptionConfig(
  @JsonProperty("algorithm")
  @field:NotNull
  @field:SchemaConstant(ALGO_RSA)
  override val algorithm: String,
  @JsonProperty("targetField")
  @field:NotNull
  override val targetField: String,
  @JsonProperty("fieldNameSuffix")
  override val fieldNameSuffix: String?,
  @JsonProperty("publicKey")
  @field:NotNull
  val publicKey: String,
) : EncryptionConfig()
