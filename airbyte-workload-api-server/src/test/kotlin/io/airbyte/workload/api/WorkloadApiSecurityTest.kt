/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.core.util.SupplierUtil
import io.micronaut.http.HttpMethod
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-only-jwt-signature-secret-1234567890")
@MicronautTest(environments = [Environment.TEST])
class WorkloadApiSecurityTest(
  embeddedServer: EmbeddedServer,
) {
  private val client = SupplierUtil.memoizedNonEmpty { embeddedServer.applicationContext.createBean(HttpClient::class.java, embeddedServer.url) }

  @Test
  fun `POST log upload authorization rejects an unauthenticated request`() {
    val error =
      assertThrows<HttpClientResponseException> {
        client
          .get()
          .toBlocking()
          .exchange(
            HttpRequest.create<Any>(HttpMethod.POST, "/api/v1/workload/1/log-upload-authorization"),
            String::class.java,
          )
      }

    assertEquals(HttpStatus.UNAUTHORIZED, error.status)
  }
}
