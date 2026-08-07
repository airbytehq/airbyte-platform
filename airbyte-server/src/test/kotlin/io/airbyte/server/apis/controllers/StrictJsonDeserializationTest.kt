/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.benmanes.caffeine.cache.Ticker
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableStrictJsonDeserialization
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.server.assertStatus
import io.airbyte.server.config.StrictJsonDeserializationFlag
import io.airbyte.server.services.DataplaneService
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.airbyte.server.wrapped.InstanceConfigurationSetupTracker
import io.airbyte.server.wrapped.apis.controllers.WrappedOrganizationUpdateTracker
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicLong

@MicronautTest(rebuildContext = true)
@Property(name = "spec.name", value = "StrictJsonDeserializationTest")
internal class StrictJsonDeserializationTest {
  @Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun connectionsHandler(): ConnectionsHandler = mockk()

    @Singleton
    @Primary
    fun organizationsHandler(): OrganizationsHandler = mockk()

    @Singleton
    @Primary
    fun dataplaneGroupService(): DataplaneGroupService = mockk()

    @Singleton
    @Primary
    fun dataplaneService(): DataplaneService = mockk()

    @Singleton
    @Primary
    fun entitlementService(): EntitlementService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun featureFlagClient(): FeatureFlagClient = mockk<TestClient>(relaxed = true)

    @Singleton
    fun strictJsonDeserializationTicker(): StrictJsonDeserializationTicker = StrictJsonDeserializationTicker()

    @Singleton
    fun strictJsonDeserializationRefreshExecutor(): StrictJsonDeserializationRefreshExecutor = StrictJsonDeserializationRefreshExecutor()

    @Singleton
    @Primary
    fun strictJsonDeserializationFlag(
      featureFlagClient: FeatureFlagClient,
      ticker: StrictJsonDeserializationTicker,
      refreshExecutor: StrictJsonDeserializationRefreshExecutor,
    ): StrictJsonDeserializationFlag = StrictJsonDeserializationFlag(featureFlagClient, refreshExecutor, ticker)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var connectionsHandler: ConnectionsHandler

  @Inject
  lateinit var organizationsHandler: OrganizationsHandler

  @Inject
  lateinit var dataplaneGroupService: DataplaneGroupService

  @Inject
  lateinit var dataplaneService: DataplaneService

  @Inject
  lateinit var entitlementService: EntitlementService

  @Inject
  lateinit var featureFlagClient: FeatureFlagClient

  @Inject
  lateinit var strictJsonDeserializationFlag: StrictJsonDeserializationFlag

  @Inject
  lateinit var strictJsonDeserializationTicker: StrictJsonDeserializationTicker

  @Inject
  lateinit var strictJsonDeserializationRefreshExecutor: StrictJsonDeserializationRefreshExecutor

  @Inject
  lateinit var wrappedOrganizationUpdateTracker: WrappedOrganizationUpdateTracker

  @Inject
  lateinit var instanceConfigurationSetupTracker: InstanceConfigurationSetupTracker

  @Inject
  lateinit var objectMapper: ObjectMapper

  @BeforeEach
  fun setUp() {
    clearMocks(connectionsHandler, organizationsHandler, dataplaneGroupService, dataplaneService, entitlementService, featureFlagClient)
    wrappedOrganizationUpdateTracker.reset()
    instanceConfigurationSetupTracker.reset()
    every { connectionsHandler.getConnection(any()) } returns ConnectionRead()
    every { organizationsHandler.updateOrganization(any()) } returns OrganizationRead()
    every { featureFlagClient.boolVariation(any(), Empty) } returns true
  }

  @Test
  fun `strict deserialization flag defaults to disabled`() {
    assertAll(
      { assertEquals("platform.enable-strict-json-deserialization", EnableStrictJsonDeserialization.key) },
      { assertFalse(EnableStrictJsonDeserialization.default) },
      { assertFalse(TestClient(emptyMap()).boolVariation(EnableStrictJsonDeserialization, Empty)) },
    )
  }

  @Test
  fun `server object mapper remains lenient`() {
    assertFalse(objectMapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES))
  }

  @Test
  fun `organization request uses stale false while refreshing and becomes strict after publication`() {
    val organizationId = UUID.randomUUID()
    val body =
      mapOf(
        "organizationId" to organizationId.toString(),
        "organization_id" to UUID.randomUUID().toString(),
        "organizationName" to "Updated name",
      )

    strictJsonDeserializationTicker.advancePastRefreshInterval()
    assertStatus(
      HttpStatus.OK,
      client.status(HttpRequest.POST("/api/v1/organizations/update", body)),
    )
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }

    strictJsonDeserializationRefreshExecutor.runAll()

    assertStatus(
      HttpStatus.BAD_REQUEST,
      client.statusException(HttpRequest.POST("/api/v1/organizations/update", body)),
    )

    assertEquals(1, wrappedOrganizationUpdateTracker.count())
    verify(exactly = 1) { organizationsHandler.updateOrganization(any()) }
    verify(exactly = 1) { featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty) }
  }

  @Test
  fun `direct replacement of a controller outside the allowlist remains lenient`() {
    enableStrictDeserialization()
    assertStatus(
      HttpStatus.NO_CONTENT,
      client.status(
        HttpRequest.POST(
          "/api/v1/instance_configuration/setup",
          mapOf(
            "email" to "test@example.com",
            "anonymousDataCollection" to false,
            "initialSetupComplete" to true,
            "displaySetupWizard" to false,
            "unknown" to "rejected",
          ),
        ),
      ),
    )
    assertEquals(1, instanceConfigurationSetupTracker.count())
  }

  @Test
  fun `direct interface replacement accepts declared properties and invokes the controller once`() {
    enableStrictDeserialization()
    assertStatus(
      HttpStatus.NO_CONTENT,
      client.status(
        HttpRequest.POST(
          "/api/v1/instance_configuration/setup",
          mapOf(
            "email" to "test@example.com",
            "anonymousDataCollection" to false,
            "initialSetupComplete" to true,
            "displaySetupWizard" to false,
          ),
        ),
      ),
    )
    assertEquals(1, instanceConfigurationSetupTracker.count())
  }

  @Test
  fun `replacement organizations update rejects conflicting organization id aliases before invoking controller or handler`() {
    enableStrictDeserialization()
    val organizationId = UUID.randomUUID()
    val body =
      mapOf(
        "organizationId" to organizationId.toString(),
        "organization_id" to UUID.randomUUID().toString(),
        "organizationName" to "Updated name",
      )

    assertStatus(
      HttpStatus.BAD_REQUEST,
      client.statusException(HttpRequest.POST("/api/v1/organizations/update", body)),
    )
    assertEquals(0, wrappedOrganizationUpdateTracker.count())
    verify(exactly = 0) { organizationsHandler.updateOrganization(any()) }
  }

  @Test
  fun `organizations update accepts declared properties and invokes the handler once`() {
    enableStrictDeserialization()
    val organizationId = UUID.randomUUID()

    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          "/api/v1/organizations/update",
          mapOf(
            "organizationId" to organizationId.toString(),
            "organizationName" to "Updated name",
          ),
        ),
      ),
    )
    assertEquals(1, wrappedOrganizationUpdateTracker.count())
    verify(exactly = 1) {
      organizationsHandler.updateOrganization(
        match {
          it.organizationId == organizationId && it.organizationName == "Updated name"
        },
      )
    }
  }

  @Test
  fun `connections get rejects an unknown property before invoking the handler`() {
    enableStrictDeserialization()
    val connectionId = UUID.randomUUID()
    val body =
      mapOf(
        "connectionId" to connectionId.toString(),
        "organization_id" to UUID.randomUUID().toString(),
      )

    assertStatus(
      HttpStatus.BAD_REQUEST,
      client.statusException(HttpRequest.POST("/api/v1/connections/get", body)),
    )
    verify(exactly = 0) { connectionsHandler.getConnection(any()) }
  }

  @Test
  fun `connections get continues to accept its declared property`() {
    enableStrictDeserialization()
    val connectionId = UUID.randomUUID()
    every { connectionsHandler.getConnection(connectionId) } returns ConnectionRead()

    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          "/api/v1/connections/get",
          mapOf("connectionId" to connectionId.toString()),
        ),
      ),
    )
    verify(exactly = 1) { connectionsHandler.getConnection(connectionId) }
  }

  @Test
  fun `controller outside the legacy api layer remains lenient`() {
    val connectionId = UUID.randomUUID()
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          "/api/test/strict-json-deserialization/lenient",
          mapOf("connectionId" to connectionId.toString()),
        ),
      ),
    )
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          "/api/test/strict-json-deserialization/lenient",
          mapOf(
            "connectionId" to connectionId.toString(),
            "unknown" to "accepted",
          ),
        ),
      ),
    )
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
  }

  @Test
  fun `dataplane api accepts a declared snake case property`() {
    enableStrictDeserialization()
    val organizationId = UUID.randomUUID()
    val dataplaneGroup =
      DataplaneGroup().apply {
        id = UUID.randomUUID()
        this.organizationId = organizationId
        name = "Test dataplane group"
        enabled = true
        createdAt = 0
        updatedAt = 0
      }
    every { entitlementService.ensureEntitled(OrganizationId(organizationId), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.writeDataplaneGroup(any()) } answers {
      firstArg<DataplaneGroup>().apply {
        id = dataplaneGroup.id
        createdAt = dataplaneGroup.createdAt
        updatedAt = dataplaneGroup.updatedAt
      }
    }
    every { dataplaneService.listDataplanes(dataplaneGroup.id) } returns emptyList()

    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          "/api/v1/dataplane_group/create",
          mapOf(
            "organization_id" to organizationId.toString(),
            "name" to dataplaneGroup.name,
            "enabled" to dataplaneGroup.enabled,
          ),
        ),
      ),
    )
    verify(exactly = 1) {
      dataplaneGroupService.writeDataplaneGroup(
        match { it.organizationId == organizationId },
      )
    }
  }

  private fun enableStrictDeserialization() {
    strictJsonDeserializationTicker.advancePastRefreshInterval()
    assertFalse(strictJsonDeserializationFlag.isEnabled())
    strictJsonDeserializationRefreshExecutor.runAll()
    assertEquals(true, strictJsonDeserializationFlag.isEnabled())
  }
}

internal class StrictJsonDeserializationTicker : Ticker {
  private val nanos = AtomicLong()

  override fun read(): Long = nanos.get()

  fun advancePastRefreshInterval() {
    nanos.addAndGet(Duration.ofSeconds(31).toNanos())
  }
}

internal class StrictJsonDeserializationRefreshExecutor : Executor {
  private val tasks = ConcurrentLinkedQueue<Runnable>()

  override fun execute(command: Runnable) {
    tasks.add(command)
  }

  fun runAll() {
    while (true) {
      val task = tasks.poll() ?: return
      task.run()
    }
  }
}
