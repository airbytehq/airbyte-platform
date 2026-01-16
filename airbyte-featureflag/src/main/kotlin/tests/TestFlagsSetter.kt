/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag.tests

import io.airbyte.commons.json.Jsons
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Flag
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

class TestFlagsSetter(
  baseUrl: String,
) {
  private val basePath = "/api/v1/feature-flags"
  private val httpClient = OkHttpClient().newBuilder().build()
  private val urlPrefix = "${baseUrl.removeSuffix("/")}$basePath"

  class FlagOverride<T>(
    private val flag: Flag<T>,
    context: Context? = null,
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

  class FlagRuleOverride<T>(
    private val flag: Flag<T>,
    private val context: Context,
    value: T,
    private val testFlags: TestFlagsSetter,
  ) : AutoCloseable {
    init {
      testFlags.setRule(flag, context, value)
    }

    override fun close() {
      testFlags.deleteRule(flag, context)
    }
  }

  fun <T> withFlag(
    flag: Flag<T>,
    value: T,
    context: Context? = null,
  ) = FlagOverride(flag, context, value, this)

  fun <T> deleteFlag(flag: Flag<T>) {
    httpClient
      .newCall(
        Request
          .Builder()
          .url("$urlPrefix/${flag.key}")
          .delete()
          .build(),
      ).execute()
  }

  fun <T> withRule(
    flag: Flag<T>,
    context: Context,
    value: T,
  ) = FlagRuleOverride(flag, context, value, this)

  fun <T> setFlag(
    flag: Flag<T>,
    context: Context? = null,
    value: T,
  ) {
    val requestFlag =
      ApiFeatureFlag(
        key = flag.key,
        default = flag.default.toString(),
        rules =
          if (context != null) {
            listOf(
              ApiRule(
                context = ApiContext(kind = context.kind, value = context.key),
                value = value.toString(),
              ),
            )
          } else {
            emptyList()
          },
      )
    val response =
      httpClient
        .newCall(
          Request
            .Builder()
            .url(urlPrefix)
            .put(Jsons.serialize(requestFlag).toRequestBody("application/json".toMediaType()))
            .build(),
        ).execute()
    assert(response.code == 200) { "Failed to update the feature flag ${requestFlag.key}, error: ${response.code}: ${response.body?.string()}" }
  }

  fun <T> getFlag(flag: Flag<T>): String? =
    httpClient
      .newCall(
        Request
          .Builder()
          .url("$urlPrefix/${flag.key}")
          .build(),
      ).execute()
      .body
      ?.string()

  fun <T> evalFlag(
    flag: Flag<T>,
    context: Context,
  ): String? =
    httpClient
      .newCall(
        Request
          .Builder()
          .url("$urlPrefix/${flag.key}/evaluate?kind=${context.kind}&value=${context.key}")
          .build(),
      ).execute()
      .body
      ?.string()

  fun <T> setRule(
    flag: Flag<T>,
    context: Context,
    value: T,
  ) {
    val requestRule =
      ApiRule(
        context = ApiContext(kind = context.kind, value = context.key),
        value = value.toString(),
      )
    val response =
      httpClient
        .newCall(
          Request
            .Builder()
            .url("$urlPrefix/${flag.key}/rules")
            .post(Jsons.serialize(requestRule).toRequestBody("application/json".toMediaType()))
            .build(),
        ).execute()
    assert(response.code == 200) { "Failed to update the feature flag ${flag.key}, error: ${response.code}: ${response.body?.string()}" }
  }

  fun <T> deleteRule(
    flag: Flag<T>,
    context: Context,
  ) {
    val requestContext = ApiContext(kind = context.kind, value = context.key)
    httpClient
      .newCall(
        Request
          .Builder()
          .url("$urlPrefix/${flag.key}/rules")
          .delete(Jsons.serialize(requestContext).toRequestBody("application/json".toMediaType()))
          .build(),
      ).execute()
  }

  private data class ApiContext(
    val kind: String,
    val value: String,
  )

  private data class ApiRule(
    val context: ApiContext,
    val value: String,
  )

  private data class ApiFeatureFlag(
    val key: String,
    val default: String,
    val rules: List<ApiRule> = listOf(),
  )
}
