/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Configs
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneClientCredentials
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneCredentialsService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.fabric8.kubernetes.client.KubernetesClient
import io.mockk.Called
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.slot
import io.mockk.unmockkObject
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

private val dpg =
  DataplaneGroup().apply {
    id = UUID.randomUUID()
    name = "dpg"
    organizationId = DEFAULT_ORGANIZATION_ID
    enabled = true
  }

private val dpc =
  DataplaneClientCredentials(
    id = UUID.randomUUID(),
    dataplaneId = dpg.id,
    clientId = "client id",
    clientSecret = "client secret",
  )

private val dp =
  Dataplane().apply {
    id = UUID.randomUUID()
    dataplaneGroupId = dpc.id
    name = "dp name"
  }

private const val SECRET_NAME = "secret name"

class DataplaneInitializerTest {
  private val service = mockk<DataplaneService>()
  private val groupService = mockk<DataplaneGroupService>()
  private val credsService = mockk<DataplaneCredentialsService>()
  private val k8sClient = mockk<KubernetesClient>()

  @BeforeEach
  fun setup() {
    mockkObject(K8sSecretHelper)
    every { K8sSecretHelper.createOrUpdateSecret(any(), any(), any()) } returns Unit
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
    unmockkObject(K8sSecretHelper)
  }

  @Test
  fun `data plane is created if none exists`() {
    every { groupService.listDataplaneGroups(DEFAULT_ORGANIZATION_ID, false) } returns listOf(dpg)
    every { service.listDataplanes(dpg.id, false) } returns emptyList()
    val dpSlot = slot<Dataplane>()
    every { service.writeDataplane(capture(dpSlot)) } answers { dpSlot.captured }
    every { credsService.createCredentials(any()) } returns dpc

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.writeDataplane(dpSlot.captured)
      credsService.createCredentials(dpSlot.captured.id)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          SECRET_NAME_CLIENT_ID to dpc.clientId,
          SECRET_NAME_CLIENT_SECRET to dpc.clientSecret,
        ),
      )
    }
  }

  @Test
  fun `data plane is not created if one exists`() {
    every { groupService.listDataplaneGroups(DEFAULT_ORGANIZATION_ID, false) } returns listOf(dpg)
    every { service.listDataplanes(dpg.id, false) } returns listOf(dp)

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
      )

    initializer.createDataplaneIfNotExists()

    verify(exactly = 0) { service.writeDataplane(any()) }
    verify {
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }

  @Test
  fun `data plane is not created running on cloud`() {
    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.CLOUD,
        secretName = SECRET_NAME,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      groupService wasNot Called
      service wasNot Called
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }

  @Test
  fun `data plane is not created if no group exists`() {
    every { groupService.listDataplaneGroups(DEFAULT_ORGANIZATION_ID, false) } returns emptyList()

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service wasNot Called
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }

  @Test
  fun `data plane is not created if more than one group exists`() {
    every { groupService.listDataplaneGroups(DEFAULT_ORGANIZATION_ID, false) } returns listOf(dpg, dpg)

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service wasNot Called
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }
}
