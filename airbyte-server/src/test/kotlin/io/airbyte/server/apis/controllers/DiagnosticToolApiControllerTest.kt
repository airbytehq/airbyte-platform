/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DiagnosticReportRequestBody
import io.airbyte.commons.server.handlers.DiagnosticToolHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.fabric8.kubernetes.client.KubernetesClient
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import java.io.File

@MicronautTest
internal class DiagnosticToolApiControllerTest {
  @Inject
  lateinit var diagnosticToolHandler: DiagnosticToolHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(KubernetesClient::class)
  fun kubernetesClient(): KubernetesClient = mockk()

  @Test
  fun testGenerateDiagnosticReport() {
    val result =
      File.createTempFile("test-diagnostic", "").also {
        it.deleteOnExit()
      }
    every { diagnosticToolHandler.generateDiagnosticReport() } returns result

    val path = "/api/v1/diagnostic_tool/generate_report"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DiagnosticReportRequestBody())))
  }
}
