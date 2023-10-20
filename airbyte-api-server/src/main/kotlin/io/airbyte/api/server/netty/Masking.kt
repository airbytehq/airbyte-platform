package io.airbyte.api.server.netty

import java.util.Locale
import java.util.stream.Collectors
import java.util.stream.Stream

private const val STRING_FIELD_MATCH_REGEX = "(\"%s\": *)(\".*?[^\\\\]\")( *[, \\n\\r}]?)"

private val GENERIC_MASKING_TERMS =
  listOf(
    "credentials", "credential", "secret", "token", "apikey", "private_key",
    "private_key_password", "api_key", "site_api_key", "ssh_key", "api_password", "api_secret", "password", "personal_access_token",
    "credentials_json", "developer_token", "tunnel_user_password", "auth_token", "api_token", "auth_user_password", "auth_ssh_key",
  )
private val OAUTH_MASKING_TERMS = listOf("client_id", "client_secret", "refresh_token", "access_token")
private val AWS_MASKING_TERMS =
  listOf(
    "aws_key_id", "aws_secret_key", "secret_key", "access_key", "aws_access_key_id",
    "aws_secret_access_key", "accessKey", "privateKey", "key_encrypting_key",
  )
private val AZURE_MASKING_TERMS = listOf("storage_access_key", "azure_blob_storage_account_key")

private val DATABRICKS_MASKING_TERMS =
  listOf("databricks_personal_access_token", "s3_access_key_id", "s3_secret_access_key", "azure_blob_storage_sas_token")
private val DYNAMO_DB_MASKING_TERMS = listOf("access_key_id", "secret_access_key")
private val NETSUITE_MASKING_TERMS = listOf("consumer_secret", "token_secret")
private val ZOOM_MASKING_TERMS = listOf("jwt_token")
private val ELASTICSEARCH_MASKING_TERMS = listOf("apiKeyId", "apiKeySecret")
private val GCS_MASKING_TERMS = listOf("hmac_key_access_id", "hmac_key_secret")

private val FIELDS_TO_MASK =
  Stream.of(
    GENERIC_MASKING_TERMS, OAUTH_MASKING_TERMS, AWS_MASKING_TERMS, AZURE_MASKING_TERMS,
    DATABRICKS_MASKING_TERMS, DYNAMO_DB_MASKING_TERMS, NETSUITE_MASKING_TERMS, ZOOM_MASKING_TERMS, ELASTICSEARCH_MASKING_TERMS,
    GCS_MASKING_TERMS,
  )
    .flatMap { x: List<String> -> x.stream() }
    .collect(Collectors.toList())

private val HEADERS_TO_MASK = listOf("Authorization")

const val DEFAULT_MASK = "__masked__"
val fieldStringMasks = FIELDS_TO_MASK.associate { field -> field.lowercase() to DEFAULT_MASK }.toMap()
val headerStringMasks = HEADERS_TO_MASK.associate { header -> header.lowercase() to DEFAULT_MASK }.toMap()

fun maskBody(
  body: String,
  contentType: String?,
): String {
  var maskedBody = body
  if (contentType.isNullOrBlank() || !contentType.lowercase(Locale.getDefault()).contains("application/json")) {
    return body
  }

  for ((key, value) in fieldStringMasks) {
    maskedBody =
      Regex(java.lang.String.format(STRING_FIELD_MATCH_REGEX, key)).replace(maskedBody) {
        return@replace "${it.groupValues[1]}\"${value}\"${it.groupValues[3]}"
      }
  }
  return maskedBody
}

fun maskHeaders(headers: Map<String, List<String>>?): Map<String, Any> {
  return headers!!.map { (k, v) ->
    if (headerStringMasks.containsKey(k.lowercase())) k to headerStringMasks.getOrDefault(k, "Null") else k to v
  }.toMap()
}
