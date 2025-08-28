/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.manifestserver.api.client

import dev.failsafe.RetryPolicy
import io.airbyte.manifestserver.api.client.generated.CapabilitiesApi
import io.airbyte.manifestserver.api.client.generated.HealthApi
import io.airbyte.manifestserver.api.client.generated.ManifestApi
import okhttp3.OkHttpClient
import okhttp3.Response

/**
 * This class wraps all the generated Manifest Server API clients and provides a single entry point.
 * This class is meant to consolidate all our Manifest Server API endpoints into a fluent-ish client.
 *
 * Our open API generators create a separate class per API "root-route". This class wraps those
 * generated APIs to provide a unified interface for interacting with the Manifest Server service.
 *
 * The client is designed to:
 * 1. Use Kotlin for type safety and modern language features
 * 2. Use OkHttp3 for robust HTTP networking
 * 3. Integrate Failsafe for circuit breaking and retry policies
 *
 * This needs to be open so that we can wrap it in micronaut test annotations for mock injection in tests.
 */
open class ManifestServerApiClient(
  basePath: String,
  policy: RetryPolicy<Response>,
  httpClient: OkHttpClient,
) {
  val capabilitiesApi = CapabilitiesApi(basePath = basePath, client = httpClient, policy = policy)
  val healthApi = HealthApi(basePath = basePath, client = httpClient, policy = policy)
  val manifestApi = ManifestApi(basePath = basePath, client = httpClient, policy = policy)
}
