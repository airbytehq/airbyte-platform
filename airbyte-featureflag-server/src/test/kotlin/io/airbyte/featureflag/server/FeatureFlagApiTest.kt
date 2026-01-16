/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag.server

import io.airbyte.commons.json.Jsons
import io.airbyte.featureflag.server.model.Context
import io.airbyte.featureflag.server.model.FeatureFlag
import io.airbyte.featureflag.server.model.Rule
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.env.Environment
import io.micronaut.core.util.SupplierUtil
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@MicronautTest(environments = [Environment.TEST])
class FeatureFlagApiTest(
  val embeddedServer: EmbeddedServer,
) {
  private val ffs = mockk<FeatureFlagService>()
  private val client = SupplierUtil.memoizedNonEmpty { embeddedServer.applicationContext.createBean(HttpClient::class.java, embeddedServer.url) }

  @MockBean(FeatureFlagService::class)
  @Replaces(FeatureFlagService::class)
  fun featureFlagService(): FeatureFlagService = ffs

  @Test
  fun `test evaluation with a context`() {
    val key = "test-eval"
    val evalResult = "eval-result"
    every { ffs.eval(key, mapOf("c" to "c1", "w" to "w1")) } returns evalResult

    val response = call<String>(HttpRequest.GET("/api/v1/feature-flags/$key/evaluate?kind=c&value=c1&kind=w&value=w1"))
    assertEquals(200, response.status.code)
    assertEquals(evalResult, response.body.get())
  }

  @Test
  fun `test delete flag`() {
    val key = "test-delete"
    every { ffs.delete(key) } returns Unit

    val response = call<String>(HttpRequest.DELETE("/api/v1/feature-flags/$key"))
    assertEquals(200, response.status.code)
  }

  @Test
  fun `test evaluation with an empty context`() {
    val key = "test-eval"
    val evalResult = "eval-result"
    every { ffs.eval(key, mapOf()) } returns evalResult

    val response = call<String>(HttpRequest.GET("/api/v1/feature-flags/$key/evaluate"))
    assertEquals(200, response.status.code)
    assertEquals(evalResult, response.body.get())
  }

  @Test
  fun `test get returns the response`() {
    val flag = FeatureFlag(key = "my-flag", default = "default")
    every { ffs.get("my-flag") } returns flag

    val response = call<FeatureFlag>(HttpRequest.GET("/api/v1/feature-flags/my-flag"))
    assertEquals(200, response.status.code)
    assertEquals(flag, response.body.get())
  }

  @Test
  fun `test get not found`() {
    every { ffs.get(any()) } returns null

    val response = callError(HttpRequest.GET("/api/v1/feature-flags/not-found-flag"))
    assertEquals(404, response.status.code)
  }

  @Test
  fun `test put`() {
    val flag = FeatureFlag(key = "flag", default = "default", rules = listOf(Rule(context = Context(kind = "c", value = "c1"), value = "c1v")))
    every { ffs.put(flag) }.returns(flag)

    val response = call<FeatureFlag>(HttpRequest.PUT("/api/v1/feature-flags/", Jsons.serialize(flag)))
    assertEquals(200, response.status.code)
    assertEquals(flag, response.body.get())
  }

  @Test
  fun `test rule delete`() {
    val context = Context(kind = "r", value = "r1")
    val flag = FeatureFlag(key = "rule-test", default = "some default")
    every { ffs.removeRule(flag.key, context) }.returns(flag)

    val response = call<FeatureFlag>(HttpRequest.DELETE("/api/v1/feature-flags/${flag.key}/rules", Jsons.serialize(context)))
    assertEquals(200, response.status.code)
    assertEquals(flag, response.body.get())
  }

  @Test
  fun `test rule post`() {
    val rule = Rule(context = Context(kind = "r", value = "r1"), value = "r1-value")
    val flag = FeatureFlag(key = "rule-test", default = "some default")
    every { ffs.addRule(flag.key, rule) }.returns(flag)

    val response = call<FeatureFlag>(HttpRequest.POST("/api/v1/feature-flags/${flag.key}/rules", Jsons.serialize(rule)))
    assertEquals(200, response.status.code)
    assertEquals(flag, response.body.get())
  }

  @Test
  fun `test rule put`() {
    val rule = Rule(context = Context(kind = "r", value = "r1"), value = "r1-value")
    val flag = FeatureFlag(key = "rule-test", default = "some default")
    every { ffs.updateRule(flag.key, rule) }.returns(flag)

    val response = call<FeatureFlag>(HttpRequest.PUT("/api/v1/feature-flags/${flag.key}/rules", Jsons.serialize(rule)))
    assertEquals(200, response.status.code)
    assertEquals(flag, response.body.get())
  }

  private inline fun <reified T> call(request: HttpRequest<Any>): HttpResponse<T> = client.get().toBlocking().exchange(request, T::class.java)

  private fun callError(request: HttpRequest<Any>): HttpResponse<*> {
    val exception = assertThrows<HttpClientResponseException> { call<String>(request) }
    return exception.response
  }
}
