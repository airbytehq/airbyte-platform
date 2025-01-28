/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import okhttp3.Interceptor

/**
 * Custom interface that extends the OkHttp3 {@link Interceptor} interface
 * for dependency injection purposes.  This allows for DI configuration to
 * differentiate between interceptors that should be used by the different
 * API clients (Airbyte API, Workload API, etc.).
 */
interface AirbyteApiInterceptor : Interceptor
