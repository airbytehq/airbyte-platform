package io.airbyte.workload.launcher.client

import com.auth0.jwt.JWT
import com.auth0.jwt.JWTCreator
import com.auth0.jwt.algorithms.Algorithm
import com.google.auth.oauth2.ServiceAccountCredentials
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.io.FileInputStream
import java.security.interfaces.RSAPrivateKey
import java.time.Duration
import java.util.Date

private val FIVE_MINUTES: Duration = Duration.ofMinutes(5L)
private val LOGGER = KotlinLogging.logger {}

/**
 * TODO: Copied from worker. This will be replaced once we are hitting the actual workload API.
 */
@Singleton
class JwtAuthenticationInterceptor(
  @Value("\${airbyte.workload-api.auth-header.name}") private val airbyteApiAuthHeaderName: String,
  @Value("\${airbyte.control-plane-auth-endpoint}") private val controlPlaneAuthEndpoint: String,
  @Value("\${airbyte.workload-api.service-account.email}") private val dataPlaneServiceAccountEmail: String,
  @Value("\${airbyte.workload-api.service-account.credentials-path}") private val dataPlaneServiceAccountCredentialsPath: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()
    builder.header("User-Agent", "WorkloadLauncherApp")
    if (airbyteApiAuthHeaderName.isNotBlank()) {
      builder.header(airbyteApiAuthHeaderName, getToken())
    }

    val updatedRequest: Request = builder.build()
    return chain.proceed(updatedRequest)
  }

  private fun getToken(): String {
    try {
      val issueDate: Date = Date()
      val expTime: Date =
        Date(System.currentTimeMillis() + FIVE_MINUTES.toMillis())
      val token: JWTCreator.Builder =
        JWT.create()
          .withIssuedAt(issueDate)
          .withExpiresAt(expTime)
          .withIssuer(dataPlaneServiceAccountEmail)
          .withAudience(controlPlaneAuthEndpoint)
          .withClaim("email", dataPlaneServiceAccountEmail)

      val stream: FileInputStream = FileInputStream(dataPlaneServiceAccountCredentialsPath)
      val cred: ServiceAccountCredentials = ServiceAccountCredentials.fromStream(stream)
      val key: RSAPrivateKey = cred.privateKey as RSAPrivateKey
      val algorithm: Algorithm = Algorithm.RSA256(null, key)
      return "Bearer " + token.sign(algorithm)
    } catch (e: Exception) {
      LOGGER.error(e) {
        "An issue occurred while generating a data plane auth token. Defaulting to empty string."
      }
      return ""
    }
  }
}
