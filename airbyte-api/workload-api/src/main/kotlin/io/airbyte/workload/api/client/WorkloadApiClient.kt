/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.airbyte.workload.api.WorkloadApiClient
import jakarta.inject.Singleton

/**
 * This class wraps all the generated Workload API clients and provides a single entry point. This class is meant
 * to consolidate all our Workload API endpoints into a fluent-ish client. Our OpenAPI generators create a separate
 * class per API "root-route". For example, if our API has two routes "/v1/First/get" and "/v1/Second/get",
 * OpenAPI generates (essentially) the following files:
 * <p>
 * ApiClient.java, FirstApi.java, SecondApi.java
 * <p>
 * To call the API type-safely, we'd do new FirstApi(new ApiClient()).get() or new SecondApi(new
 * ApiClient()).get(), which can get cumbersome if we're interacting with many pieces of the API.
 * <p>
 * Our new JVM (kotlin) client is designed to do a few things:
 * <ol>
 * <li>1. Use kotlin!</li>
 * <li>2. Use OkHttp3 instead of the native java client (The native one dies on any network blip. OkHttp
 * is more robust and smooths over network blips).</li>
 * <li>3. Integrate failsafe (https://failsafe.dev/) for circuit breaking / retry<li>
 * policies.
 * </ol>
 * <p>
 */

@Singleton
@Deprecated("use WorkloadApiOperations directly", ReplaceWith("io.airbyte.workload.api.WorkloadApiClient"))
class WorkloadApiClient(
  val workloadApi: WorkloadApiClient,
)
