/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.US_DATAPLANE_GROUP
import io.airbyte.config.Configs
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.ServiceAccountNotFound
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.airbyte.domain.models.ServiceAccount
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

private val DATAPLANE_ID = UUID.randomUUID()
private const val DATAPLANE_SECRET = "secret"

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

private val serviceAccount =
  ServiceAccount(
    id = DATAPLANE_ID,
    name = "service-account",
    secret = DATAPLANE_SECRET,
  )

private val dp =
  Dataplane().apply {
    id = UUID.randomUUID()
    dataplaneGroupId = dpg.id
    name = "dp name"
    serviceAccountId = serviceAccount.id
  }

private val dataplaneWithServiceAccount =
  DataplaneWithServiceAccount(
    dataplane = dp,
    serviceAccount = serviceAccount,
  )

private const val SECRET_NAME = "secret name"
private const val CLIENT_ID_SECRET_KEY = "client-id-key"
private const val CLIENT_SECRET_SECRET_KEY = "client-secret-key"
private const val JOBS_NAMESPACE = "jobs"

class DataplaneInitializerTest {
  private val service = mockk<DataplaneService>()
  private val groupService = mockk<DataplaneGroupService>()
  private val k8sClient = mockk<KubernetesClient>()
  private val serviceAccountsService = mockk<ServiceAccountsService>()

  @BeforeEach
  fun setup() {
    mockkObject(K8sSecretHelper)
    every { K8sSecretHelper.createOrUpdateSecret(any(), any(), any()) } returns Unit
    every { K8sSecretHelper.copySecretToNamespace(any(), any(), any(), any()) } returns Unit
    every { K8sSecretHelper.getAndDecodeSecret(any(), any()) } returns null
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
    every { service.createDataplaneAndServiceAccount(capture(dpSlot), true) } answers { dataplaneWithServiceAccount }

    val initializer =
      DataplaneInitializer(
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.createDataplaneAndServiceAccount(dpSlot.captured, true)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          CLIENT_ID_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.id.toString(),
          CLIENT_SECRET_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.secret,
        ),
      )
    }
  }

  @Test
  fun `data plane is created if secret credentials don't match`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns listOf(dpg)
    every { service.listDataplanes(dpg.id, false) } returns emptyList()
    val dpSlot = slot<Dataplane>()
    every { service.createDataplaneAndServiceAccount(capture(dpSlot), true) } answers { dataplaneWithServiceAccount }
    every { K8sSecretHelper.getAndDecodeSecret(any(), SECRET_NAME) } returns
      mapOf(
        CLIENT_ID_SECRET_KEY to "foo",
        CLIENT_SECRET_SECRET_KEY to "bar",
      )

    val initializer =
      DataplaneInitializer(
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.createDataplaneAndServiceAccount(dpSlot.captured, true)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          CLIENT_ID_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.id.toString(),
          CLIENT_SECRET_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.secret,
        ),
      )
    }
  }

  @Test
  fun `data plane is created if credentials exist but service account doesn't exist`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns listOf(dpg)
    every { service.listDataplanes(dpg.id, false) } returns emptyList()
    val dpSlot = slot<Dataplane>()
    every { service.createDataplaneAndServiceAccount(capture(dpSlot), true) } answers { dataplaneWithServiceAccount }
    every { K8sSecretHelper.getAndDecodeSecret(any(), SECRET_NAME) } returns
      mapOf(
        CLIENT_ID_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.id.toString(),
        CLIENT_SECRET_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.secret,
      )
    every { serviceAccountsService.getAndVerify(any(), any()) } throws ServiceAccountNotFound(UUID.randomUUID())

    val initializer =
      DataplaneInitializer(
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.createDataplaneAndServiceAccount(dpSlot.captured, true)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          CLIENT_ID_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.id.toString(),
          CLIENT_SECRET_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.secret,
        ),
      )
    }
  }

  @Test
  fun `data plane is not created if valid credentials exist`() {
    every { serviceAccountsService.getAndVerify(any(), any()) } returns serviceAccount
    every { K8sSecretHelper.getAndDecodeSecret(any(), SECRET_NAME) } returns
      mapOf(
        CLIENT_ID_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.id.toString(),
        CLIENT_SECRET_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.secret,
      )

    val initializer =
      DataplaneInitializer(
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    initializer.createDataplaneIfNotExists()

    verify(exactly = 0) { service.createDataplaneAndServiceAccount(any(), any()) }
  }

  @Test
  fun `dataplane is created for US dataplane group on Cloud and secret copied to jobs namespace`() {
    every { groupService.getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP) } returns dpgUS
    every { service.listDataplanes(dpgUS.id, false) } returns emptyList()
    val dpSlot = slot<Dataplane>()
    every { service.createDataplaneAndServiceAccount(capture(dpSlot), true) } answers { dataplaneWithServiceAccount }
    every { k8sClient.namespace } returns "ab"

    val initializer =
      DataplaneInitializer(
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.CLOUD,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service.createDataplaneAndServiceAccount(dpSlot.captured, true)
      K8sSecretHelper.createOrUpdateSecret(
        k8sClient,
        SECRET_NAME,
        mapOf(
          CLIENT_ID_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.id.toString(),
          CLIENT_SECRET_SECRET_KEY to dataplaneWithServiceAccount.serviceAccount.secret,
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
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    shouldThrow<IllegalStateException> { initializer.createDataplaneIfNotExists() }

    verify {
      service wasNot Called
      k8sClient wasNot Called
    }
  }

  @Test
  fun `data plane is not created if more than one group exists`() {
    every { groupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false) } returns listOf(dpg, dpg)

    val initializer =
      DataplaneInitializer(
        dataplaneService = service,
        groupService = groupService,
        k8sClient = k8sClient,
        edition = Configs.AirbyteEdition.COMMUNITY,
        secretName = SECRET_NAME,
        clientIdSecretKey = CLIENT_ID_SECRET_KEY,
        clientSecretSecretKey = CLIENT_SECRET_SECRET_KEY,
        jobsNamespace = JOBS_NAMESPACE,
        serviceAccountsService = serviceAccountsService,
      )

    initializer.createDataplaneIfNotExists()

    verify {
      service wasNot Called
      k8sClient wasNot Called
    }
  }
}
