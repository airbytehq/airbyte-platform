/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.micronaut.http.HttpRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.util.UUID

class RequestReferencesTest {
  @Test
  fun `uses body identifiers and ignores client reference headers`() {
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val request =
      HttpRequest
        .POST("/api/v1/sources/get", mapOf("sourceId" to sourceId, "workspaceIds" to listOf(workspaceId)))
        .header(AuthenticationId.SOURCE_ID.httpHeader, UUID.randomUUID().toString())
        .header(AuthenticationId.WORKSPACE_ID.httpHeader, UUID.randomUUID().toString())

    val references = RequestReferences.from(request)

    assertEquals(sourceId.toString(), references.values[AuthenticationId.SOURCE_ID])
    assertEquals("[\"$workspaceId\"]", references.values[AuthenticationId.WORKSPACE_IDS])
    assertFalse(AuthenticationId.WORKSPACE_ID in references.values)
  }

  @Test
  fun `reads identifiers from JSON text bodies`() {
    val workspaceId = UUID.randomUUID()
    val request = HttpRequest.POST("/api/v1/sources/list", """{"workspaceId":"$workspaceId"}""")

    assertEquals(mapOf(AuthenticationId.WORKSPACE_ID to workspaceId.toString()), RequestReferences.from(request).values)
  }

  @Test
  fun `reads identifiers from JSON byte bodies`() {
    val workspaceId = UUID.randomUUID()
    val body = """{"workspaceId":"$workspaceId"}""".toByteArray(Charsets.UTF_8)
    val request = HttpRequest.POST("/api/v1/sources/list", body)

    assertEquals(mapOf(AuthenticationId.WORKSPACE_ID to workspaceId.toString()), RequestReferences.from(request).values)
  }

  @Test
  fun `ignores reference headers without body identifiers`() {
    val request = HttpRequest.GET<Any>("/api/v1/health")
    AuthenticationId.entries.forEach { request.header(it.httpHeader, UUID.randomUUID().toString()) }

    assertEquals(emptyMap<AuthenticationId, String>(), RequestReferences.from(request).values)
  }

  @Test
  fun `ignores empty null and malformed JSON bodies`() {
    for (body in listOf("", "null", "{", "{}", "[]", "0", """{"workspaceId":null,"sourceId":""}""")) {
      val request = HttpRequest.POST("/api/v1/sources/list", body)

      assertEquals(emptyMap<AuthenticationId, String>(), RequestReferences.from(request).values, body)
    }
  }
}
