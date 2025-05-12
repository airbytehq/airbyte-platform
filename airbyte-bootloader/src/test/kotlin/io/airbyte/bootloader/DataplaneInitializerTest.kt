/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.constants.US_DATAPLANE_GROUP
import io.airbyte.config.Configs
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneClientCredentials
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneCredentialsService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.fabric8.kubernetes.client.KubernetesClient
import io.kotest.assertions.throwables.shouldThrow
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

private val dpgUS =
  DataplaneGroup().apply {
    id = UUID.randomUUID()
    name = US_DATAPLANE_GROUP
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
private const val CLIENT_ID_SECRET_KEY = "client-id-key"
private const val CLIENT_SECRET_SECRET_KEY = "client-secret-key"
private const val JOBS_NAMESPACE = "jobs"

class DataplaneInitializerTest {
  private val service = mockk<DataplaneService>()
  private val groupService = mockk<DataplaneGroupService>()
  private val credsService = mockk<DataplaneCredentialsService>()
  private val k8sClient = mockk<KubernetesClient>()

  @BeforeEach
  fun setup() {
    mockkObject(K8sSecretHelper)
    every { K8sSecretHelper.createOrUpdateSecret(any(), any(), any()) } returns Unit
    every { K8sSecretHelper.copySecretToNamespace(any(), any(), any(), any()) } returns Unit
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
    unmockkObject(K8sSecretHelper)
  }

  @Test
  fun `data plane is created if none exists`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns listOf(dpg)
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
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.writeDataplane(dpSlot.captured)
      credsService.createCredentials(dpSlot.captured.id)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          CLIENT_ID_SECRET_KEY to dpc.clientId,
          CLIENT_SECRET_SECRET_KEY to dpc.clientSecret,
        ),
      )
    }
  }

  @Test
  fun `data plane is not created if one exists`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns listOf(dpg)
    every { service.listDataplanes(dpg.id, false) } returns listOf(dp)

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
      )

    initializer.createDataplaneIfNotExists()

    verify(exactly = 0) { service.writeDataplane(any()) }
    verify {
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }

  @Test
  fun `dataplane is created for US dataplane group on Cloud and secret copied to jobs namespace`() {
    every { groupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP) } returns dpgUS
    every { service.listDataplanes(dpgUS.id, false) } returns emptyList()
    val dpSlot = slot<Dataplane>()
    every { service.writeDataplane(capture(dpSlot)) } answers { dpSlot.captured }
    every { credsService.createCredentials(any()) } returns dpc
    every { k8sClient.namespace } returns "ab"

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.CLOUD,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.writeDataplane(dpSlot.captured)
      credsService.createCredentials(dpSlot.captured.id)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          CLIENT_ID_SECRET_KEY to dpc.clientId,
          CLIENT_SECRET_SECRET_KEY to dpc.clientSecret,
        ),
      )
      K8sSecretHelper.copySecretToNamespace(
        k8sClient,
        SECRET_NAME,
        "ab",
        JOBS_NAMESPACE,
      )
    }
  }

  @Test
  fun `data plane is not created and exception is thrown if no group exists`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns emptyList()

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
      )

    shouldThrow<IllegalStateException> { initializer.createDataplaneIfNotExists() }

    verify {
      service wasNot Called
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }

  @Test
  fun `data plane is not created if more than one group exists`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns listOf(dpg, dpg)

    val initializer =
      DataplaneInitializer(
        service = service,
        groupService = groupService,
        dataplaneCredentialsService = credsService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service wasNot Called
      credsService wasNot Called
      k8sClient wasNot Called
    }
  }
}
