/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.client.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.SourceDefinitionCreate
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteApiClient
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.security.GeneralSecurityException
import java.util.Map
import java.util.Optional
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tags(Tag("sync"), Tag("enterprise"))
internal class VersioningAcceptanceTests {
  @ParameterizedTest
  @CsvSource(
    "2.1.1, 0.2.0",
    "2.1.2, 0.2.1",
  )
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateSourceSpec(
    dockerImageTag: String,
    expectedProtocolVersion: String?,
  ) {
    val srcDefCreate =
      CustomSourceDefinitionCreate(
        SourceDefinitionCreate(
          "Source E2E Test Connector",
          "airbyte/source-e2e-test",
          dockerImageTag,
          URI("https://hub.docker.com/r/airbyte/source-e2e-test"),
          null,
          null,
        ),
        workspaceId,
        null,
        null,
      )
    val srcDefRead = apiClient2.sourceDefinitionApi.createCustomSourceDefinition(srcDefCreate)
    Assertions.assertEquals(expectedProtocolVersion, srcDefRead.protocolVersion)

    val srcDefReq = SourceDefinitionIdRequestBody(srcDefRead.sourceDefinitionId)
    val srcDefReadSanityCheck = apiClient2.sourceDefinitionApi.getSourceDefinition(srcDefReq)
    Assertions.assertEquals(srcDefRead.protocolVersion, srcDefReadSanityCheck.protocolVersion)

    // Clean up the source
    apiClient2.sourceDefinitionApi.deleteSourceDefinition(srcDefReq)
  }

  @ParameterizedTest
  @CsvSource(
    "2.1.1, 0.2.0",
    "2.1.2, 0.2.1",
  )
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateDestinationSpec(
    dockerImageTag: String,
    expectedProtocolVersion: String?,
  ) {
    val dstDefCreate =
      CustomDestinationDefinitionCreate(
        DestinationDefinitionCreate(
          "Dest E2E Test Connector",
          "airbyte/source-e2e-test",
          dockerImageTag,
          URI("https://hub.docker.com/r/airbyte/destination-e2e-test"),
          null,
          null,
        ),
        workspaceId,
        null,
        null,
      )

    val dstDefRead = apiClient2.destinationDefinitionApi.createCustomDestinationDefinition(dstDefCreate)
    Assertions.assertEquals(expectedProtocolVersion, dstDefRead.protocolVersion)

    val dstDefReq = DestinationDefinitionIdRequestBody(dstDefRead.destinationDefinitionId)
    val dstDefReadSanityCheck = apiClient2.destinationDefinitionApi.getDestinationDefinition(dstDefReq)
    Assertions.assertEquals(dstDefRead.protocolVersion, dstDefReadSanityCheck.protocolVersion)

    // Clean up the destination
    apiClient2.destinationDefinitionApi.deleteDestinationDefinition(dstDefReq)
  }

  companion object {
    lateinit var apiClient2: AirbyteApiClient
    lateinit var workspaceId: UUID
    private val AIRBYTE_SERVER_HOST: String = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001")

    @BeforeAll
    @Throws(
      IOException::class,
      GeneralSecurityException::class,
      URISyntaxException::class,
      InterruptedException::class,
    )
    @JvmStatic
    fun setup() {
      apiClient2 = createAirbyteApiClient(String.format("%s/api", AIRBYTE_SERVER_HOST), Map.of())

      val acceptanceTestsResources = AcceptanceTestsResources()
      acceptanceTestsResources.init()
      workspaceId = acceptanceTestsResources.workspaceId
    }
  }
}
