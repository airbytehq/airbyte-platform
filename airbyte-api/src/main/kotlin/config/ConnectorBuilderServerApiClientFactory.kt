package config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.auth.InternalApiAuthenticationInterceptor
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response
import java.time.Duration

@Factory
class ConnectorBuilderServerApiClientFactory {
  @Singleton
  fun connectorBuilderServerApiClient(
    @Value("\${airbyte.connector-builder-server-api.base-path}") connectorBuilderServerApiBasePath: String,
    @Value("\${airbyte.connector-builder-server-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.connector-builder-server-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    authenticationInterceptor: InternalApiAuthenticationInterceptor,
  ): ConnectorBuilderServerApi {
    val builder: OkHttpClient.Builder =
      OkHttpClient.Builder().apply {
        addInterceptor(authenticationInterceptor)
        readTimeout(Duration.ofSeconds(readTimeoutSeconds))
        connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
      }

    val okHttpClient: OkHttpClient = builder.build()
    val retryPolicy: RetryPolicy<Response> = RetryPolicy.builder<Response>().withMaxRetries(0).build()

    return ConnectorBuilderServerApi(basePath = connectorBuilderServerApiBasePath, policy = retryPolicy, client = okHttpClient)
  }
}
