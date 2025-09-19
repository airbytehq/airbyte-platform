/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.test.AtcConfig
import io.airbyte.test.PassFail
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("api")
class ApiAcceptanceTest {
  private val atClient = AcceptanceTestClient()

  @BeforeAll
  fun setup() {
    atClient.setup()
  }

  @AfterAll
  fun tearDownAll() {
    atClient.tearDownAll()
  }

  @AfterEach
  fun tearDown() {
    atClient.tearDown()
  }

  @Test
  fun `non existent resource returns a 404`() {
    val e = assertThrows<ClientException> { atClient.admin.getSource(UUID.randomUUID()) }
    assertEquals(404, e.statusCode)
  }

  @Test
  fun `source check passes correctly`() {
    val sourceId = atClient.admin.createAtcSource()
    val resp = atClient.admin.checkSource(sourceId)
    assertEquals(CheckConnectionRead.Status.SUCCEEDED, resp.status)
    assertEquals("check passed (expected)", resp.message)

    val source = atClient.admin.getSource(sourceId)
    assertTrue(source.name.startsWith(NAME_PREFIX), "name starts with $NAME_PREFIX")
    assertEquals(source.workspaceId, atClient.workspaceId)

    val sourceDef = atClient.admin.getSourceDefinition(source.sourceDefinitionId)
    assertEquals(CUSTOM_DOCKER_REPO, sourceDef.dockerRepository)
    assertEquals(CUSTOM_DOCKER_TAG, sourceDef.dockerImageTag)

    val sourceDefSpec = atClient.admin.getSourceDefinitionSpec(sourceDef.sourceDefinitionId)
    // TODO(cole): actually verify the specification matches the expected response
    assertNotNull(sourceDefSpec.connectionSpecification, "missing connector specification")
  }

  @Test
  fun `source check fails correctly`() {
    val sourceId = atClient.admin.createAtcSource(AtcConfig(check = PassFail.Fail))
    val resp = atClient.admin.checkSource(sourceId)
    assertEquals(CheckConnectionRead.Status.FAILED, resp.status)
    assertEquals("check failed (expected)", resp.message)
  }

  @Test
  fun `update source passes correctly`() {
    val sourceId = atClient.admin.createAtcSource()

    val source = atClient.admin.getSource(sourceId)
    assertEquals("custom", source.connectionConfiguration["dataset"].asText())

    val updatedSourceId = atClient.admin.updateAtcSource(sourceId, AtcConfig(dataset = "movies"))
    assertEquals(sourceId, updatedSourceId)

    val updatedSource = atClient.admin.getSource(updatedSourceId)
    assertEquals("movies", updatedSource.connectionConfiguration["dataset"].asText())
  }

  @Test
  fun `update source fails correctly`() {
    val sourceId = atClient.admin.createAtcSource()

    val source = atClient.admin.getSource(sourceId)
    assertEquals("custom", source.connectionConfiguration["dataset"].asText())

    assertThrows<Throwable> { atClient.admin.updateAtcSource(sourceId, AtcConfig(dataset = "games", check = PassFail.Fail)) }

    val nonUpdatedSource = atClient.admin.getSource(sourceId)
    // source was _not_ updated
    assertEquals("custom", nonUpdatedSource.connectionConfiguration["dataset"].asText())
  }

  @Test
  fun `source discover passes correctly`() {
    val sourceId = atClient.admin.createAtcSource()
    val res = atClient.admin.discoverSource(sourceId)
    assertEquals(1, res.catalog?.streams?.size)
    assertEquals(
      "stream-custom",
      res.catalog
        ?.streams[0]
        ?.stream
        ?.name,
    )
    assertEquals(
      listOf("film"),
      res.catalog
        ?.streams[0]
        ?.stream
        ?.defaultCursorField,
    )
  }

  @Test
  fun `source discover fails correctly`() {
    val sourceId = atClient.admin.createAtcSource(AtcConfig(discover = PassFail.Fail))
    assertThrows<IllegalStateException> { atClient.admin.discoverSource(sourceId) }
  }

  @Test
  fun `destination check passes correctly`() {
    val destinationId = atClient.admin.createAtcDestination()
    val resp = atClient.admin.checkDestination(destinationId)
    assertEquals(CheckConnectionRead.Status.SUCCEEDED, resp.status)
    assertEquals("check passed (expected)", resp.message)
  }

  @Test
  fun `destination check fails correctly`() {
    val destinationId = atClient.admin.createAtcDestination(AtcConfig(check = PassFail.Fail))
    val resp = atClient.admin.checkDestination(destinationId)
    assertEquals(CheckConnectionRead.Status.FAILED, resp.status)
    assertEquals("check failed (expected)", resp.message)
  }

  @Test
  fun `destination discovery passes correctly`() {
    // TODO
  }

  @Test
  fun `destination discovery fails correctly`() {
    // TODO
  }

  @Test
  fun `create connection`() {
    val connectionId = atClient.admin.createAtcConnection()
    assertNotNull(connectionId)
  }
}
