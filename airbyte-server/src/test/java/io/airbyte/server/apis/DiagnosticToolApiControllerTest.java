/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.DiagnosticReportRequestBody;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Micronaut-based test suite for the {@link ConnectionApiController} class.
 */
@MicronautTest
@Requires(env = {Environment.KUBERNETES, Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class DiagnosticToolApiControllerTest extends BaseControllerTest {

  @MockBean(KubernetesClient.class)
  KubernetesClient kubernetesClient() {
    return Mockito.mock(KubernetesClient.class);
  }

  @Test
  void testGenerateDiagnosticReport() throws IOException {
    final File result = File.createTempFile("test-diagnostic", "");
    result.deleteOnExit();
    Mockito.when(diagnosticToolHandler.generateDiagnosticReport()).thenReturn(result);
    final String path = "/api/v1/diagnostic_tool/generate_report";
    testEndpointStatus(
        HttpRequest.POST(path, new DiagnosticReportRequestBody()),
        HttpStatus.OK);
  }

}
