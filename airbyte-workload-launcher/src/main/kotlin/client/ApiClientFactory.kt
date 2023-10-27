package io.airbyte.workload.launcher.client

import dev.failsafe.RetryPolicy
import io.airbyte.api.client2.AirbyteApiClient2
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.time.Duration

@Factory
class ApiClientFactory {
  @Singleton
  fun airbyteApiClient(
    @Value("\${airbyte.workload-api.basepath}") airbyteWorloadApiBasePath: String,
    jwtAuthenticationInterceptor: JwtAuthenticationInterceptor,
  ): AirbyteApiClient2 {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    builder.addInterceptor(jwtAuthenticationInterceptor)
    builder.readTimeout(Duration.ofSeconds(300))
    builder.connectTimeout(Duration.ofSeconds(30))

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
        .withDelay(Duration.ofSeconds(2))
        .withMaxRetries(5)
        .build()

    return AirbyteApiClient2(airbyteWorloadApiBasePath, retryPolicy, okHttpClient)
  }
}
