/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.logging

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.WorkloadType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FlexSyncLogging
import io.airbyte.featureflag.Organization
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.micronaut.runtime.StorageType
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.ForbiddenException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.KnownException
import io.airbyte.workload.errors.LogUploadAuthorizationBrokerException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.handler.WorkloadHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import java.time.OffsetDateTime
import java.util.UUID

class LogUploadAuthorizationServiceTest {
  private val workloadHandler = mockk<WorkloadHandler>()
  private val roleResolver = mockk<io.airbyte.commons.server.authorization.RoleResolver>()
  private val authorizationRequest = mockk<io.airbyte.commons.server.authorization.RoleResolver.Request>()
  private val dataplaneService = mockk<DataplaneService>()
  private val dataplaneGroupService = mockk<DataplaneGroupService>()
  private val featureFlagClient = mockk<FeatureFlagClient>()
  private val broker = mockk<LogUploadCredentialBroker>()
  private val meterRegistry = SimpleMeterRegistry()
  private val service =
    LogUploadAuthorizationService(
      workloadHandler = workloadHandler,
      roleResolver = roleResolver,
      dataplaneService = dataplaneService,
      dataplaneGroupService = dataplaneGroupService,
      featureFlagClient = featureFlagClient,
      storageConfig =
        AirbyteStorageConfig(
          type = StorageType.GCS,
          bucket = AirbyteStorageConfig.AirbyteStorageBucketConfig(log = BUCKET),
        ),
      credentialBroker = broker,
      metricClient = MetricClient(meterRegistry),
    )

  @BeforeEach
  fun setUp() {
    every { roleResolver.newRequest() } returns authorizationRequest
    every { authorizationRequest.withCurrentAuthentication() } returns authorizationRequest
    every { authorizationRequest.subject } returns
      io.airbyte.commons.server.authorization.RoleResolver
        .Subject(SERVICE_ACCOUNT_ID.toString(), TokenType.SERVICE_ACCOUNT)
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload()
    every { dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString()) } returns dataplane()
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } returns dataplaneGroup()
    every { featureFlagClient.boolVariation(FlexSyncLogging, Organization(ORGANIZATION_ID)) } returns true
    every { broker.issue(BUCKET, OBJECT_PREFIX) } returns AUTHORIZATION
  }

  @ParameterizedTest
  @MethodSource("eligibleActiveWorkloads")
  fun `issues authorization for every eligible workload type in every active status`(
    type: WorkloadType,
    status: WorkloadStatus,
  ) {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload(type = type, status = status)

    assertEquals(AUTHORIZATION, service.authorize(WORKLOAD_ID))

    verifyAll {
      dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString())
      dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID)
      featureFlagClient.boolVariation(FlexSyncLogging, Organization(ORGANIZATION_ID))
      broker.issue(BUCKET, OBJECT_PREFIX)
    }
    assertMetric("issued")
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadType::class, names = ["SYNC", "CHECK", "DISCOVER"])
  fun `returns no authorization for every eligible workload type when the workload organization flag is disabled`(type: WorkloadType) {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload(type = type)
    every { featureFlagClient.boolVariation(FlexSyncLogging, Organization(ORGANIZATION_ID)) } returns false

    assertNull(service.authorize(WORKLOAD_ID))

    verify(exactly = 0) { broker.issue(any(), any()) }
    assertMetric("disabled")
  }

  @Test
  fun `uses only the managed log bucket and normalized persisted log path`() {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload(logPath = "/job/7/attempt/2///")
    every { broker.issue(BUCKET, "job-logging/job/7/attempt/2/") } returns AUTHORIZATION

    service.authorize(WORKLOAD_ID)

    verify(exactly = 1) { broker.issue(BUCKET, "job-logging/job/7/attempt/2/") }
  }

  @Test
  fun `missing workload is a sanitized not found rejection`() {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } throws NotFoundException("missing $WORKLOAD_ID")

    val error = assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    assertMetric("rejected")
  }

  @ParameterizedTest
  @MethodSource("malformedWorkloads")
  fun `missing or malformed workload identity topology is not found`(workload: Workload) {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload

    val error = assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    verify(exactly = 0) { broker.issue(any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("ineligibleTokenTypes")
  fun `authenticated non-service-account identity is not found before workload lookup`(tokenType: TokenType) {
    every { authorizationRequest.subject } returns
      io.airbyte.commons.server.authorization.RoleResolver
        .Subject(SERVICE_ACCOUNT_ID.toString(), tokenType)

    val error = assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }

    assertEquals(HttpStatus.NOT_FOUND, error.getHttpCode())
    assertSanitized(error.message)
    verify(exactly = 0) { workloadHandler.getWorkload(any()) }
    verify(exactly = 0) { broker.issue(any(), any()) }
  }

  @Test
  fun `missing authenticated identity is unauthorized before workload lookup`() {
    every { authorizationRequest.subject } returns null

    val error = assertThrows<KnownException> { service.authorize(WORKLOAD_ID) }

    assertEquals(HttpStatus.UNAUTHORIZED, error.getHttpCode())
    assertSanitized(error.message)
    verify(exactly = 0) { workloadHandler.getWorkload(any()) }
  }

  @Test
  fun `service account without a dataplane mapping is not found`() {
    every { dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString()) } returns null

    assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }
  }

  @Test
  fun `service account must map to the exact workload dataplane`() {
    every { dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString()) } returns
      dataplane(id = UUID.randomUUID())

    assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }
  }

  @Test
  fun `dataplane group must match the exact workload assignment`() {
    every { dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString()) } returns
      dataplane(groupId = UUID.randomUUID())

    assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }
  }

  @Test
  fun `organization-scoped group lookup hides a mismatched assignment`() {
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } throws
      ConfigNotFoundException("dataplane group", DATAPLANE_GROUP_ID.toString())
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, DEFAULT_ORGANIZATION_ID) } throws
      ConfigNotFoundException("dataplane group", DATAPLANE_GROUP_ID.toString())

    assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }

    verify(exactly = 1) { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) }
    verify(exactly = 1) { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, DEFAULT_ORGANIZATION_ID) }
  }

  @Test
  fun `group returned outside the requested organization is not found`() {
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } returns
      dataplaneGroup(organizationId = UUID.randomUUID())

    assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }
  }

  @Test
  fun `Airbyte default group is a sanitized conflict before flag evaluation`() {
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } throws
      ConfigNotFoundException("dataplane group", DATAPLANE_GROUP_ID.toString())
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, DEFAULT_ORGANIZATION_ID) } returns
      dataplaneGroup(organizationId = DEFAULT_ORGANIZATION_ID)

    val error = assertThrows<ConflictException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
    verify(exactly = 0) { broker.issue(any(), any()) }
    verify(exactly = 1) { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) }
    verify(exactly = 1) { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, DEFAULT_ORGANIZATION_ID) }
    verify(exactly = 0) { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID) }
  }

  @Test
  fun `Airbyte default group remains a conflict when the customer organization flag is disabled`() {
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } throws
      ConfigNotFoundException("dataplane group", DATAPLANE_GROUP_ID.toString())
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, DEFAULT_ORGANIZATION_ID) } returns
      dataplaneGroup(organizationId = DEFAULT_ORGANIZATION_ID)
    every { featureFlagClient.boolVariation(FlexSyncLogging, Organization(ORGANIZATION_ID)) } returns false

    val error = assertThrows<ConflictException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
    verify(exactly = 0) { broker.issue(any(), any()) }
  }

  @Test
  fun `group with missing organization is malformed topology and not found`() {
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } returns
      dataplaneGroup(organizationId = null)

    assertThrows<NotFoundException> { service.authorize(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @MethodSource("disabledTopologies")
  fun `disabled or tombstoned assigned topology is forbidden`(
    dataplane: Dataplane,
    group: DataplaneGroup,
  ) {
    every { dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString()) } returns dataplane
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) } returns group

    assertThrows<ForbiddenException> { service.authorize(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadType::class, names = ["SYNC", "CHECK", "DISCOVER"])
  fun `unclaimed pending eligible workload is a conflict after matching the caller to its persisted group`(type: WorkloadType) {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns
      workload(
        type = type,
        status = WorkloadStatus.PENDING,
        dataplaneId = null,
      )

    val error = assertThrows<ConflictException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    verify(exactly = 1) { dataplaneService.getDataplaneByServiceAccountId(SERVICE_ACCOUNT_ID.toString()) }
    verify(exactly = 1) { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID, ORGANIZATION_ID) }
    verify(exactly = 0) { broker.issue(any(), any()) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadType::class, mode = EnumSource.Mode.EXCLUDE, names = ["SYNC", "CHECK", "DISCOVER"])
  fun `ineligible workload type is a sanitized conflict`(type: WorkloadType) {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload(type = type)

    val error = assertThrows<ConflictException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
    verify(exactly = 0) { broker.issue(any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("eligibleTerminalWorkloads")
  fun `terminal eligible workload is gone`(
    type: WorkloadType,
    status: WorkloadStatus,
  ) {
    every { workloadHandler.getWorkload(WORKLOAD_ID) } returns workload(type = type, status = status)

    val error = assertThrows<InvalidStatusTransitionException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    verify(exactly = 0) { broker.issue(any(), any()) }
  }

  @Test
  fun `non GCS storage cannot issue a GCS authorization`() {
    val nonGcsService =
      LogUploadAuthorizationService(
        workloadHandler,
        roleResolver,
        dataplaneService,
        dataplaneGroupService,
        featureFlagClient,
        AirbyteStorageConfig(type = StorageType.MINIO),
        broker,
        MetricClient(meterRegistry),
      )

    assertThrows<NotFoundException> { nonGcsService.authorize(WORKLOAD_ID) }
  }

  @Test
  fun `broker failures are sanitized and measured`() {
    every { broker.issue(BUCKET, OBJECT_PREFIX) } throws IllegalStateException("token=$TOKEN bucket=$BUCKET prefix=$OBJECT_PREFIX")

    val error = assertThrows<LogUploadAuthorizationBrokerException> { service.authorize(WORKLOAD_ID) }

    assertSanitized(error.message)
    assertNull(error.cause)
    assertMetric("broker_error")
  }

  private fun assertMetric(outcome: String) {
    assertEquals(
      1.0,
      meterRegistry
        .find(OssMetricsRegistry.WORKLOAD_LOG_UPLOAD_AUTHORIZATION.getMetricName())
        .tags("route", ROUTE, "outcome", outcome)
        .counter()
        ?.count(),
    )
    assertEquals(
      1L,
      meterRegistry
        .find(OssMetricsRegistry.WORKLOAD_LOG_UPLOAD_AUTHORIZATION_LATENCY_MS.getMetricName())
        .tags("route", ROUTE, "outcome", outcome)
        .summary()
        ?.count(),
    )
    val tagValues = meterRegistry.meters.flatMap { it.id.tags }.map { it.value }
    assertTrue(listOf(WORKLOAD_ID, TOKEN, BUCKET, OBJECT_PREFIX).none(tagValues::contains))
  }

  private fun assertSanitized(message: String?) {
    assertEquals("Log upload authorization is unavailable.", message)
    assertFalse(listOf(WORKLOAD_ID, TOKEN, BUCKET, OBJECT_PREFIX).any { message.orEmpty().contains(it) })
  }

  companion object {
    private const val WORKLOAD_ID = "sync-123"
    private const val BUCKET = "managed-log-bucket"
    private const val TOKEN = "secret-access-token"
    private const val OBJECT_PREFIX = "job-logging/job/7/attempt/2/"
    private const val ROUTE = "/api/v1/workload/{workloadId}/log-upload-authorization"
    private val ORGANIZATION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111")
    private val DATAPLANE_ID = UUID.fromString("22222222-2222-2222-2222-222222222222")
    private val DATAPLANE_GROUP_ID = UUID.fromString("33333333-3333-3333-3333-333333333333")
    private val SERVICE_ACCOUNT_ID = UUID.fromString("44444444-4444-4444-4444-444444444444")
    private val AUTHORIZATION =
      GcsDownscopedOAuthLogUploadAuthorization(
        accessToken = TOKEN,
        expiresAt = OffsetDateTime.parse("2026-09-02T12:00:00Z"),
        bucketName = BUCKET,
        objectKeyPrefix = OBJECT_PREFIX,
      )

    @JvmStatic
    fun malformedWorkloads(): List<Workload> =
      listOf(
        workload(organizationId = null),
        workload(dataplaneId = null),
        workload(dataplaneId = "not-a-uuid"),
        workload(dataplaneGroup = null),
        workload(dataplaneGroup = "not-a-uuid"),
        workload(status = null),
        workload(logPath = "///"),
      )

    @JvmStatic
    fun disabledTopologies(): List<Array<Any>> =
      listOf(
        arrayOf(dataplane(enabled = false), dataplaneGroup()),
        arrayOf(dataplane(tombstone = true), dataplaneGroup()),
        arrayOf(dataplane(), dataplaneGroup(enabled = false)),
        arrayOf(dataplane(), dataplaneGroup(tombstone = true)),
      )

    @JvmStatic
    fun ineligibleTokenTypes(): List<TokenType> = TokenType.entries.filterNot { it == TokenType.SERVICE_ACCOUNT }

    @JvmStatic
    fun eligibleActiveWorkloads(): List<Array<Any>> =
      workloadTypeAndStatusCases(
        WorkloadStatus.CLAIMED,
        WorkloadStatus.LAUNCHED,
        WorkloadStatus.RUNNING,
      )

    @JvmStatic
    fun eligibleTerminalWorkloads(): List<Array<Any>> =
      workloadTypeAndStatusCases(
        WorkloadStatus.SUCCESS,
        WorkloadStatus.FAILURE,
        WorkloadStatus.CANCELLED,
      )

    private fun workloadTypeAndStatusCases(vararg statuses: WorkloadStatus): List<Array<Any>> =
      listOf(WorkloadType.SYNC, WorkloadType.CHECK, WorkloadType.DISCOVER).flatMap { type ->
        statuses.map { status -> arrayOf(type, status) }
      }

    private fun workload(
      organizationId: UUID? = ORGANIZATION_ID,
      dataplaneId: String? = DATAPLANE_ID.toString(),
      dataplaneGroup: String? = DATAPLANE_GROUP_ID.toString(),
      status: WorkloadStatus? = WorkloadStatus.RUNNING,
      logPath: String = "job/7/attempt/2",
      type: WorkloadType = WorkloadType.SYNC,
    ) = Workload(
      id = WORKLOAD_ID,
      organizationId = organizationId,
      dataplaneId = dataplaneId,
      dataplaneGroup = dataplaneGroup,
      status = status,
      logPath = logPath,
      type = type,
    )

    private fun dataplane(
      id: UUID = DATAPLANE_ID,
      groupId: UUID = DATAPLANE_GROUP_ID,
      enabled: Boolean? = true,
      tombstone: Boolean? = false,
    ) = Dataplane()
      .withId(id)
      .withDataplaneGroupId(groupId)
      .withServiceAccountId(SERVICE_ACCOUNT_ID)
      .withEnabled(enabled)
      .withTombstone(tombstone)

    private fun dataplaneGroup(
      organizationId: UUID? = ORGANIZATION_ID,
      enabled: Boolean? = true,
      tombstone: Boolean? = false,
    ) = DataplaneGroup()
      .withId(DATAPLANE_GROUP_ID)
      .withOrganizationId(organizationId)
      .withEnabled(enabled)
      .withTombstone(tombstone)
  }
}
