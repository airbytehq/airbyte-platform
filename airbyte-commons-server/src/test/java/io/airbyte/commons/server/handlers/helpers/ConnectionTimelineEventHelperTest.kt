/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Organization
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.User
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ConnectionAutoUpdatedReason
import io.airbyte.data.services.shared.ConnectionEvent
import io.airbyte.data.services.shared.ConnectionSettingsChangedEvent
import io.airbyte.data.services.shared.SchemaChangeAutoPropagationEvent
import io.airbyte.domain.services.storage.ConnectorObjectStorageService
import io.airbyte.persistence.job.JobPersistence
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.Optional
import java.util.UUID

class ConnectionTimelineEventHelperTest {
  private lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  private lateinit var currentUserService: CurrentUserService
  private lateinit var organizationPersistence: OrganizationPersistence
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var userPersistence: UserPersistence
  private lateinit var connectionTimelineEventService: ConnectionTimelineEventService
  private lateinit var connectorObjectStorageService: ConnectorObjectStorageService

  companion object {
    val CONNECTION_ID: UUID = UUID.randomUUID()
  }

  @BeforeEach
  fun setup() {
    currentUserService = mock()
    organizationPersistence = mock()
    permissionHandler = mock()
    userPersistence = mock()
    connectionTimelineEventService = mock()
    connectorObjectStorageService = mock()
  }

  @Test
  fun testGetTimelineJobStats() {
    connectionTimelineEventHelper =
      ConnectionTimelineEventHelper(
        setOf(),
        currentUserService,
        organizationPersistence,
        permissionHandler,
        userPersistence,
        connectorObjectStorageService,
        connectionTimelineEventService,
      )

    val userStreamName = "user"
    val purchaseStreamName = "purchase"
    val vendorStreamName = "vendor"

    val catalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(userStreamName, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            AirbyteStream(purchaseStreamName, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            AirbyteStream(vendorStreamName, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val jobConfig = JobConfig().withConfigType(JobConfig.ConfigType.SYNC).withSync(JobSyncConfig().withConfiguredAirbyteCatalog(catalog))
    val job = Job(100L, JobConfig.ConfigType.SYNC, CONNECTION_ID.toString(), jobConfig, emptyList(), JobStatus.SUCCEEDED, 0L, 0L, 0L, true)

    val perAttemptStreamStats =
      listOf(
        mapOf(
          userStreamName to SyncStats().withBytesCommitted(100L).withRecordsCommitted(1L).withRecordsRejected(0L),
          purchaseStreamName to SyncStats().withBytesCommitted(1000L).withRecordsCommitted(10L).withRecordsRejected(1L),
          vendorStreamName to SyncStats().withBytesCommitted(10000L).withRecordsCommitted(100L).withRecordsRejected(10L),
        ),
        mapOf(
          userStreamName to SyncStats().withBytesCommitted(500L).withRecordsCommitted(8L).withRecordsRejected(0L),
          purchaseStreamName to SyncStats().withBytesCommitted(5000L).withRecordsCommitted(80L).withRecordsRejected(8L),
          vendorStreamName to SyncStats().withBytesCommitted(50000L).withRecordsCommitted(800L).withRecordsRejected(80L),
        ),
        mapOf(
          userStreamName to SyncStats().withBytesCommitted(200L).withRecordsCommitted(7L).withRecordsRejected(0L),
          purchaseStreamName to SyncStats().withBytesCommitted(2000L).withRecordsCommitted(70L).withRecordsRejected(7L),
          vendorStreamName to SyncStats().withBytesCommitted(20000L).withRecordsCommitted(700L).withRecordsRejected(70L),
        ),
      )

    val attemptStatsList =
      perAttemptStreamStats.map {
        JobPersistence.AttemptStats(SyncStats(), it.map { (k, v) -> StreamSyncStats().withStreamName(k).withStats(v) })
      }

    val expectedBytesLoaded = 200L + (1000L + 5000L + 2000L) + (10000L + 50000L + 20000L)
    val expectedRecordsLoaded = 7L + (10L + 80L + 70L) + (100L + 800L + 700L)
    val expectedRecordsRejected = (1L + 8L + 7L) + (10L + 80L + 70L)

    val result = connectionTimelineEventHelper.buildTimelineJobStats(job, attemptStatsList)
    Assertions.assertEquals(expectedBytesLoaded, result.loadedBytes)
    Assertions.assertEquals(expectedRecordsLoaded, result.loadedRecords)
    Assertions.assertEquals(expectedRecordsRejected, result.rejectedRecords)
  }

  @Nested
  inner class TestGetUserReadInConnectionEvent {
    private val cloudAirbyteSupportEmailDomain = setOf("airbyte.io")
    private val ossAirbyteSupportEmailDomain = setOf<String>()
    private val airbyteUserId = UUID.randomUUID()
    private val airbyteUserName = "IAMZOZO"
    private val airbyteUserEmail = "xx@airbyte.io"
    private val airbyteUser = User().withUserId(airbyteUserId).withEmail(airbyteUserEmail).withName(airbyteUserName)
    private val userId = UUID.randomUUID()
    private val userEmail = "yy@gmail.com"
    private val userName = "yy"
    private val externalUser = User().withUserId(userId).withEmail(userEmail).withName(userName)

    @Test
    fun notApplicableInOSS() {
      connectionTimelineEventHelper =
        ConnectionTimelineEventHelper(
          ossAirbyteSupportEmailDomain,
          currentUserService,
          organizationPersistence,
          permissionHandler,
          userPersistence,
          connectorObjectStorageService,
          connectionTimelineEventService,
        )
      whenever(userPersistence.getUser(anyOrNull())).thenReturn(Optional.of(externalUser))
      whenever(permissionHandler.isUserInstanceAdmin(anyOrNull())).thenReturn(false)
      whenever(organizationPersistence.getOrganizationByConnectionId(anyOrNull())).thenReturn(Optional.of(Organization().withEmail(userEmail)))
      val userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(userId, anyOrNull())
      Assertions.assertEquals(false, userRead!!.isDeleted)
      Assertions.assertEquals(userName, userRead!!.name)
    }

    @Test
    fun airbyteSupportInAirbytersInternalWorkspace() {
      connectionTimelineEventHelper =
        ConnectionTimelineEventHelper(
          cloudAirbyteSupportEmailDomain,
          currentUserService,
          organizationPersistence,
          permissionHandler,
          userPersistence,
          connectorObjectStorageService,
          connectionTimelineEventService,
        )
      whenever(userPersistence.getUser(anyOrNull())).thenReturn(Optional.of(airbyteUser))
      whenever(permissionHandler.isUserInstanceAdmin(anyOrNull())).thenReturn(true)
      whenever(organizationPersistence.getOrganizationByConnectionId(anyOrNull())).thenReturn(Optional.of(Organization().withEmail(airbyteUserEmail)))
      val userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(airbyteUserId, anyOrNull())
      Assertions.assertEquals(airbyteUserName, userRead!!.name)
    }

    @Test
    fun airbyteSupportInCustomersExternalWorkspace() {
      connectionTimelineEventHelper =
        ConnectionTimelineEventHelper(
          cloudAirbyteSupportEmailDomain,
          currentUserService,
          organizationPersistence,
          permissionHandler,
          userPersistence,
          connectorObjectStorageService,
          connectionTimelineEventService,
        )
      whenever(userPersistence.getUser(anyOrNull())).thenReturn(Optional.of(airbyteUser))
      whenever(permissionHandler.isUserInstanceAdmin(anyOrNull())).thenReturn(true)
      whenever(organizationPersistence.getOrganizationByConnectionId(anyOrNull())).thenReturn(Optional.of(Organization().withEmail(userEmail)))
      val userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(airbyteUserId, anyOrNull())
      Assertions.assertEquals(ConnectionTimelineEventHelper.AIRBYTE_SUPPORT_USER_NAME, userRead!!.name)
    }

    @Test
    fun detectNonAirbyteSupportUserInCloud() {
      connectionTimelineEventHelper =
        ConnectionTimelineEventHelper(
          cloudAirbyteSupportEmailDomain,
          currentUserService,
          organizationPersistence,
          permissionHandler,
          userPersistence,
          connectorObjectStorageService,
          connectionTimelineEventService,
        )
      whenever(userPersistence.getUser(anyOrNull())).thenReturn(Optional.of(externalUser))
      whenever(permissionHandler.isUserInstanceAdmin(anyOrNull())).thenReturn(true)
      whenever(organizationPersistence.getOrganizationByConnectionId(anyOrNull())).thenReturn(Optional.of(Organization().withEmail(userEmail)))
      val userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(userId, anyOrNull())
      Assertions.assertEquals(false, userRead!!.isDeleted)
      Assertions.assertEquals(userName, userRead!!.name)
    }
  }

  @Test
  fun testLogConnectionSettingsChangedEvent() {
    connectionTimelineEventHelper =
      ConnectionTimelineEventHelper(
        setOf(),
        currentUserService,
        organizationPersistence,
        permissionHandler,
        userPersistence,
        connectorObjectStorageService,
        connectionTimelineEventService,
      )

    val connectionId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val originalConnectionRead =
      ConnectionRead()
        .connectionId(connectionId)
        .name("old name")
        .prefix("old prefix")
        .notifySchemaChanges(false)
        .dataplaneGroupId(dataplaneGroupId)
        .notifySchemaChangesByEmail(false)
    val patch =
      ConnectionUpdate()
        .connectionId(connectionId)
        .name("new name")
        .prefix("new prefix")
        .dataplaneGroupId(dataplaneGroupId)
        .notifySchemaChanges(true)

    val expectedPatches =
      mapOf(
        "name" to mapOf("from" to "old name", "to" to "new name"),
        "prefix" to mapOf("from" to "old prefix", "to" to "new prefix"),
        "notifySchemaChanges" to mapOf("from" to false, "to" to true),
      )

    connectionTimelineEventHelper.logConnectionSettingsChangedEventInConnectionTimeline(connectionId, originalConnectionRead, patch, null, true)
    val eventCaptor = argumentCaptor<ConnectionSettingsChangedEvent>()
    verify(connectionTimelineEventService).writeEvent(eq(connectionId), eventCaptor.capture(), anyOrNull())
    val capturedEvent = eventCaptor.firstValue
    Assertions.assertNotNull(capturedEvent)
    Assertions.assertEquals(expectedPatches, capturedEvent.getPatches())
    Assertions.assertNull(capturedEvent.getUpdateReason())
    Assertions.assertEquals(ConnectionEvent.Type.CONNECTION_SETTINGS_UPDATE, capturedEvent.getEventType())
  }

  @Test
  fun testLogSchemaChangeAutoPropagationEvent() {
    connectionTimelineEventHelper =
      ConnectionTimelineEventHelper(
        setOf(),
        currentUserService,
        organizationPersistence,
        permissionHandler,
        userPersistence,
        connectorObjectStorageService,
        connectionTimelineEventService,
      )

    val connectionId = UUID.randomUUID()
    val diff = CatalogDiff().addTransformsItem(StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM))

    connectionTimelineEventHelper.logSchemaChangeAutoPropagationEventInConnectionTimeline(connectionId, diff)
    val eventCaptor = argumentCaptor<SchemaChangeAutoPropagationEvent>()
    verify(connectionTimelineEventService).writeEvent(eq(connectionId), eventCaptor.capture(), anyOrNull())
    val capturedEvent: SchemaChangeAutoPropagationEvent = eventCaptor.firstValue

    Assertions.assertNotNull(capturedEvent)
    Assertions.assertEquals(diff, capturedEvent.getCatalogDiff())
    Assertions.assertEquals(ConnectionAutoUpdatedReason.SCHEMA_CHANGE_AUTO_PROPAGATE.name, capturedEvent.getUpdateReason())
    Assertions.assertEquals(ConnectionEvent.Type.SCHEMA_UPDATE, capturedEvent.getEventType())
  }
}
