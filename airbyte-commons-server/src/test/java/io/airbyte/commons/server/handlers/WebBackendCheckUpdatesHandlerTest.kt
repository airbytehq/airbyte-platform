/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.specs.RemoteDefinitionsProvider
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.util.Map
import java.util.UUID

internal class WebBackendCheckUpdatesHandlerTest {
  var sourceDefinitionsHandler: SourceDefinitionsHandler? = null
  var destinationDefinitionsHandler: DestinationDefinitionsHandler? = null
  var remoteDefinitionsProvider: RemoteDefinitionsProvider? = null
  var webBackendCheckUpdatesHandler: WebBackendCheckUpdatesHandler? = null

  @BeforeEach
  fun beforeEach() {
    sourceDefinitionsHandler = Mockito.mock(SourceDefinitionsHandler::class.java)
    destinationDefinitionsHandler = Mockito.mock(DestinationDefinitionsHandler::class.java)
    remoteDefinitionsProvider = Mockito.mock(RemoteDefinitionsProvider::class.java)
    webBackendCheckUpdatesHandler =
      WebBackendCheckUpdatesHandler(sourceDefinitionsHandler!!, destinationDefinitionsHandler!!, remoteDefinitionsProvider!!)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testCheckWithoutUpdate() {
    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val sourceTag1 = "1.0.0"
    val sourceTag2 = "2.0.0"

    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()
    val destTag1 = "0.1.0"
    val destTag2 = "0.2.0"

    setMocks(
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(
        Map.entry<UUID?, String?>(source1, sourceTag1),
        Map.entry<UUID?, String?>(source2, sourceTag2),
        Map.entry<UUID?, String?>(source2, sourceTag2),
      ),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(
        Map.entry<UUID?, String?>(source1, sourceTag1),
        Map.entry<UUID?, String?>(source2, sourceTag2),
      ),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, destTag1), Map.entry<UUID?, String?>(dest2, destTag2)),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, destTag1), Map.entry<UUID?, String?>(dest2, destTag2)),
    )

    val actual = webBackendCheckUpdatesHandler!!.checkUpdates()

    Assertions.assertEquals(WebBackendCheckUpdatesRead().destinationDefinitions(0).sourceDefinitions(0), actual)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testCheckWithUpdate() {
    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val sourceTag1 = "1.1.0"
    val sourceTag2 = "2.1.0"

    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()
    val destTag1 = "0.1.0"
    val destTag2 = "0.2.0"

    setMocks(
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(
        Map.entry<UUID?, String?>(source1, sourceTag1),
        Map.entry<UUID?, String?>(source2, sourceTag2),
        Map.entry<UUID?, String?>(source2, sourceTag2),
      ),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(
        Map.entry<UUID?, String?>(source1, "1.1.1"),
        Map.entry<UUID?, String?>(source2, sourceTag2),
      ),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(
        Map.entry<UUID?, String?>(dest1, destTag1),
        Map.entry<UUID?, String?>(dest2, destTag2),
        Map.entry<UUID?, String?>(dest2, destTag2),
      ),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, destTag1), Map.entry<UUID?, String?>(dest2, "0.3.0")),
    )

    val actual = webBackendCheckUpdatesHandler!!.checkUpdates()

    Assertions.assertEquals(WebBackendCheckUpdatesRead().destinationDefinitions(2).sourceDefinitions(1), actual)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testCheckWithMissingActorDefFromLatest() {
    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val sourceTag1 = "1.0.0"
    val sourceTag2 = "2.0.0"

    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()
    val destTag1 = "0.1.0"
    val destTag2 = "0.2.0"

    setMocks(
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(
        Map.entry<UUID?, String?>(source1, sourceTag1),
        Map.entry<UUID?, String?>(source2, sourceTag2),
        Map.entry<UUID?, String?>(source2, sourceTag2),
      ),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(source2, sourceTag2)),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, destTag1), Map.entry<UUID?, String?>(dest2, destTag2)),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, destTag1)),
    )

    val actual = webBackendCheckUpdatesHandler!!.checkUpdates()

    Assertions.assertEquals(WebBackendCheckUpdatesRead().destinationDefinitions(0).sourceDefinitions(0), actual)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testCheckErrorNoCurrentDestinations() {
    setMocksForExceptionCases()
    Mockito
      .`when`(destinationDefinitionsHandler!!.listDestinationDefinitions())
      .thenThrow(IOException("unable to read current destinations"))

    val actual = webBackendCheckUpdatesHandler!!.checkUpdates()

    Assertions.assertEquals(WebBackendCheckUpdatesRead().destinationDefinitions(0).sourceDefinitions(1), actual)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testCheckErrorNoCurrentSources() {
    setMocksForExceptionCases()
    Mockito
      .`when`(sourceDefinitionsHandler!!.listSourceDefinitions())
      .thenThrow(IOException("unable to read current sources"))

    val actual = webBackendCheckUpdatesHandler!!.checkUpdates()

    Assertions.assertEquals(WebBackendCheckUpdatesRead().destinationDefinitions(1).sourceDefinitions(0), actual)
  }

  @Throws(IOException::class, InterruptedException::class)
  private fun setMocksForExceptionCases() {
    val source1 = UUID.randomUUID()
    val sourceTag1 = source1.toString()

    val dest1 = UUID.randomUUID()
    val destTag1 = dest1.toString()

    setMocks(
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(source1, sourceTag1)),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(source1, UUID.randomUUID().toString())),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, destTag1)),
      listOf<MutableMap.MutableEntry<UUID?, String?>?>(Map.entry<UUID?, String?>(dest1, UUID.randomUUID().toString())),
    )
  }

  @Throws(IOException::class, InterruptedException::class)
  private fun setMocks(
    currentSources: List<MutableMap.MutableEntry<UUID?, String?>?>,
    latestSources: List<MutableMap.MutableEntry<UUID?, String?>?>,
    currentDestinations: List<MutableMap.MutableEntry<UUID?, String?>?>,
    latestDestinations: List<MutableMap.MutableEntry<UUID?, String?>?>,
  ) {
    Mockito
      .`when`(sourceDefinitionsHandler!!.listSourceDefinitions())
      .thenReturn(
        SourceDefinitionReadList().sourceDefinitions(
          currentSources
            .stream()
            .map { idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>? ->
              this.createSourceDef(
                idImageTagEntry!!,
              )
            }.toList(),
        ),
      )
    Mockito
      .`when`(remoteDefinitionsProvider!!.getSourceDefinitions())
      .thenReturn(
        latestSources
          .stream()
          .map { idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>? ->
            this.createRegistrySourceDef(
              idImageTagEntry!!,
            )
          }.toList(),
      )

    Mockito
      .`when`(destinationDefinitionsHandler!!.listDestinationDefinitions())
      .thenReturn(
        DestinationDefinitionReadList().destinationDefinitions(
          currentDestinations
            .stream()
            .map { idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>? ->
              this.createDestinationDef(
                idImageTagEntry!!,
              )
            }.toList(),
        ),
      )
    Mockito
      .`when`(remoteDefinitionsProvider!!.getDestinationDefinitions())
      .thenReturn(
        latestDestinations
          .stream()
          .map { idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>? ->
            this.createRegistryDestinationDef(
              idImageTagEntry!!,
            )
          }.toList(),
      )
  }

  private fun createRegistryDestinationDef(idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>): ConnectorRegistryDestinationDefinition =
    ConnectorRegistryDestinationDefinition()
      .withDestinationDefinitionId(idImageTagEntry.key)
      .withDockerImageTag(idImageTagEntry.value)

  private fun createDestinationDef(idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>): DestinationDefinitionRead =
    DestinationDefinitionRead()
      .destinationDefinitionId(idImageTagEntry.key)
      .dockerImageTag(idImageTagEntry.value)

  private fun createRegistrySourceDef(idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>): ConnectorRegistrySourceDefinition =
    ConnectorRegistrySourceDefinition()
      .withSourceDefinitionId(idImageTagEntry.key)
      .withDockerImageTag(idImageTagEntry.value)

  private fun createSourceDef(idImageTagEntry: MutableMap.MutableEntry<UUID?, String?>): SourceDefinitionRead =
    SourceDefinitionRead()
      .sourceDefinitionId(idImageTagEntry.key)
      .dockerImageTag(idImageTagEntry.value)
}
