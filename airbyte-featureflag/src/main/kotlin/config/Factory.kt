/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag.config

import com.launchdarkly.sdk.server.LDClient
import io.airbyte.featureflag.CONFIG_FF_APIKEY
import io.airbyte.featureflag.CONFIG_FF_CLIENT
import io.airbyte.featureflag.CONFIG_FF_CLIENT_VAL_FFS
import io.airbyte.featureflag.CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient

@Factory
class Factory {
  @Singleton
  @Requires(property = CONFIG_FF_CLIENT, value = CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY)
  fun ldClient(
    @Property(name = CONFIG_FF_APIKEY) apiKey: String,
  ): LDClient = LDClient(apiKey)

  @Singleton
  @Requires(property = CONFIG_FF_CLIENT, value = CONFIG_FF_CLIENT_VAL_FFS)
  @Named("ffsHttpClient")
  fun ffsHttpClient(): OkHttpClient = OkHttpClient.Builder().build()
}
