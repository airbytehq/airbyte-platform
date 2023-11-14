/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import dev.failsafe.RetryPolicy
import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.time.Duration

@Factory
class WorkloadApiClientFactory {
  @Singleton
  fun workloadApiClient(
    @Named("internalApiScheme") internalApiScheme: String,
    @Value("\${airbyte.workload-api.base-path}") workloadApiBasePath: String,
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    @Value("\${airbyte.workload-api.retries.delay-seconds}") retryDelaySeconds: Long,
    @Value("\${airbyte.workload-api.retries.max}") maxRetries: Int,
    authenticationInterceptor: AuthenticationInterceptor,
  ): WorkloadApi {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    builder.addInterceptor(authenticationInterceptor)
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))

    val okHttpClient: OkHttpClient = builder.build()

    val retryPolicy: RetryPolicy<Response> =
      RetryPolicy.builder<Response>()
        .handle(
          listOf(
            IllegalStateException::class.java,
            IOException::class.java,
            UnsupportedOperationException::class.java,
            ClientException::class.java,
            ServerException::class.java,
          ),
        )
        .withDelay(Duration.ofSeconds(retryDelaySeconds))
        .withMaxRetries(maxRetries)
        .build()

    return WorkloadApi("$internalApiScheme://$workloadApiBasePath", okHttpClient, retryPolicy)
  }
}
