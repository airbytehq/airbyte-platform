package io.airbyte.featureflag.tests

import io.airbyte.commons.json.Jsons
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Flag
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

class TestFlagsSetter {
  private val baseurl = "http://local.airbyte.dev/api/v1/feature-flags"
  private val httpClient = OkHttpClient().newBuilder().build()

  class FlagOverride<T>(
    private val flag: Flag<T>,
    context: Context,
    value: T,
    private val testFlags: TestFlagsSetter,
  ) : AutoCloseable {
    init {
      testFlags.setFlag(flag, context, value)
    }

    override fun close() {
      testFlags.deleteFlag(flag)
    }
  }

  fun <T> withFlag(
    flag: Flag<T>,
    context: Context,
    value: T,
  ) = FlagOverride(flag, context, value, this)

  fun <T> deleteFlag(flag: Flag<T>) {
    httpClient.newCall(
      Request.Builder()
        .url("$baseurl/${flag.key}")
        .delete()
        .build(),
    ).execute()
  }

  fun <T> setFlag(
    flag: Flag<T>,
    context: Context,
    value: T,
  ) {
    val requestFlag =
      ApiFeatureFlag(
        key = flag.key,
        default = flag.default.toString(),
        rules =
          listOf(
            ApiRule(
              context = ApiContext(kind = context.kind, value = context.key),
              value = value.toString(),
            ),
          ),
      )
    httpClient.newCall(
      Request.Builder()
        .url(baseurl)
        .put(Jsons.serialize(requestFlag).toRequestBody("application/json".toMediaType()))
        .build(),
    ).execute()
  }

  fun <T> getFlag(flag: Flag<T>) {
    httpClient.newCall(
      Request.Builder()
        .url("$baseurl/${flag.key}")
        .build(),
    ).execute()
  }

  fun <T> evalFlag(
    flag: Flag<T>,
    context: Context,
  ) {
    httpClient.newCall(
      Request.Builder()
        .url("$baseurl/${flag.key}/evaluate?kind=${context.kind}&value=${context.key}")
        .build(),
    ).execute()
  }

  private data class ApiContext(val kind: String, val value: String)

  private data class ApiRule(val context: ApiContext, val value: String)

  private data class ApiFeatureFlag(
    val key: String,
    val default: String,
    val rules: List<ApiRule> = listOf(),
  )
}
