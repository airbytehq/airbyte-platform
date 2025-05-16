/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.UserReadInConnectionEvent;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.Organization;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.config.User;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.shared.ConnectionAutoUpdatedReason;
import io.airbyte.data.services.shared.ConnectionEvent.Type;
import io.airbyte.data.services.shared.ConnectionSettingsChangedEvent;
import io.airbyte.data.services.shared.SchemaChangeAutoPropagationEvent;
import io.airbyte.persistence.job.JobPersistence;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ConnectionTimelineEventHelperTest {

  private ConnectionTimelineEventHelper connectionTimelineEventHelper;
  private CurrentUserService currentUserService;
  private OrganizationPersistence organizationPersistence;
  private PermissionHandler permissionHandler;
  private UserPersistence userPersistence;
  private ConnectionTimelineEventService connectionTimelineEventService;
  private static final UUID CONNECTION_ID = UUID.randomUUID();

  @BeforeEach
  void setup() {
    currentUserService = mock(CurrentUserService.class);
    organizationPersistence = mock(OrganizationPersistence.class);
    permissionHandler = mock(PermissionHandler.class);
    userPersistence = mock(UserPersistence.class);
    connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
  }

  @Test
  void testGetLoadedStats() {

    connectionTimelineEventHelper = new ConnectionTimelineEventHelper(Set.of(),
        currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);

    final String userStreamName = "user";
    final SyncMode userStreamMode = SyncMode.FULL_REFRESH;
    final String purchaseStreamName = "purchase";
    final SyncMode purchaseStreamMode = SyncMode.INCREMENTAL;
    final String vendorStreamName = "vendor";
    final SyncMode vendorStreamMode = SyncMode.INCREMENTAL;

    final ConfiguredAirbyteCatalog catalog = new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream(new AirbyteStream(userStreamName, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)), userStreamMode,
                DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(new AirbyteStream(purchaseStreamName, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)),
                purchaseStreamMode, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(new AirbyteStream(vendorStreamName, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)), vendorStreamMode,
                DestinationSyncMode.APPEND)));

    final JobConfig jobConfig = new JobConfig().withConfigType(SYNC).withSync(new JobSyncConfig().withConfiguredAirbyteCatalog(catalog));
    final Job job =
        new Job(100L, SYNC, CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.SUCCEEDED, 0L, 0L, 0L, true);

    /*
     * on a per stream basis, the stats are "users" -> (100L, 1L), (500L, 8L), (200L, 7L) "purchase" ->
     * (1000L, 10L), (5000L, 80L), (2000L, 70L) "vendor" -> (10000L, 100L), (50000L, 800L), (20000L,
     * 700L)
     */

    final List<Map<String, SyncStats>> perAttemptStreamStats = List.of(
        Map.of(
            userStreamName, new SyncStats().withBytesCommitted(100L).withRecordsCommitted(1L),
            purchaseStreamName, new SyncStats().withBytesCommitted(1000L).withRecordsCommitted(10L),
            vendorStreamName, new SyncStats().withBytesCommitted(10000L).withRecordsCommitted(100L)),

        Map.of(
            userStreamName, new SyncStats().withBytesCommitted(500L).withRecordsCommitted(8L),
            purchaseStreamName, new SyncStats().withBytesCommitted(5000L).withRecordsCommitted(80L),
            vendorStreamName, new SyncStats().withBytesCommitted(50000L).withRecordsCommitted(800L)),
        Map.of(
            userStreamName, new SyncStats().withBytesCommitted(200L).withRecordsCommitted(7L),
            purchaseStreamName, new SyncStats().withBytesCommitted(2000L).withRecordsCommitted(70L),
            vendorStreamName, new SyncStats().withBytesCommitted(20000L).withRecordsCommitted(700L)));

    final List<JobPersistence.AttemptStats> attemptStatsList = perAttemptStreamStats
        .stream().map(dict -> new JobPersistence.AttemptStats(
            new SyncStats(),
            dict.entrySet()
                .stream().map(entry -> new StreamSyncStats()
                    .withStreamName(entry.getKey()).withStats(entry.getValue()))
                .toList()))
        .toList();

    // For full refresh streams, on the last value matters, for other modes the bytes/records are summed
    // across syncs
    final long expectedBytesLoaded = 200L + (1000L + 5000L + 2000L) + (10000L + 50000L + 20000L);
    final long expectedRecordsLoaded = 7L + (10L + 80L + 70L) + (100L + 800L + 700L);
    final var result = connectionTimelineEventHelper.buildLoadedStats(job, attemptStatsList);
    assertEquals(expectedBytesLoaded, result.bytes());
    assertEquals(expectedRecordsLoaded, result.records());
  }

  @Nested
  class TestGetUserReadInConnectionEvent {

    final Set<String> cloudAirbyteSupportEmailDomain = Set.of("airbyte.io");
    final Set<String> ossAirbyteSupportEmailDomain = Set.of();
    final UUID airbyteUserId = UUID.randomUUID();
    final String airbyteUserName = "IAMZOZO";
    final String airbyteUserEmail = "xx@airbyte.io";
    final User airbyteUser = new User()
        .withUserId(airbyteUserId)
        .withEmail(airbyteUserEmail)
        .withName(airbyteUserName);
    final UUID userId = UUID.randomUUID();
    final String userEmail = "yy@gmail.com";
    final String userName = "yy";
    final User externalUser = new User()
        .withUserId(userId)
        .withEmail(userEmail)
        .withName(userName);

    @Test
    void notApplicableInOSS() throws IOException {
      // No support email domains. Should show real name as always.
      connectionTimelineEventHelper = new ConnectionTimelineEventHelper(ossAirbyteSupportEmailDomain,
          currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);
      when(userPersistence.getUser(any())).thenReturn(Optional.of(externalUser));
      when(permissionHandler.isUserInstanceAdmin(any())).thenReturn(false);
      when(organizationPersistence.getOrganizationByConnectionId(any())).thenReturn(
          Optional.of(new Organization().withEmail(userEmail)));
      final UserReadInConnectionEvent userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(userId, any());
      assertEquals(false, userRead.getIsDeleted());
      assertEquals(userName, userRead.getName());
    }

    @Test
    void airbyteSupportInAirbytersInternalWorkspace() throws IOException {
      // Should show real name.
      connectionTimelineEventHelper = new ConnectionTimelineEventHelper(cloudAirbyteSupportEmailDomain,
          currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);
      when(userPersistence.getUser(any())).thenReturn(Optional.of(airbyteUser));
      when(permissionHandler.isUserInstanceAdmin(any())).thenReturn(true);
      when(organizationPersistence.getOrganizationByConnectionId(any())).thenReturn(
          Optional.of(new Organization().withEmail(airbyteUserEmail)));
      final UserReadInConnectionEvent userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(airbyteUserId, any());
      assertEquals(airbyteUserName, userRead.getName());
    }

    @Test
    void airbyteSupportInCustomersExternalWorkspace() throws IOException {
      // Should hide real name.
      connectionTimelineEventHelper = new ConnectionTimelineEventHelper(cloudAirbyteSupportEmailDomain,
          currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);
      when(userPersistence.getUser(any())).thenReturn(Optional.of(airbyteUser));
      when(permissionHandler.isUserInstanceAdmin(any())).thenReturn(true);
      when(organizationPersistence.getOrganizationByConnectionId(any())).thenReturn(
          Optional.of(new Organization().withEmail(userEmail)));
      final UserReadInConnectionEvent userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(airbyteUserId, any());
      assertEquals(ConnectionTimelineEventHelper.AIRBYTE_SUPPORT_USER_NAME, userRead.getName());
    }

    @Test
    void detectNonAirbyteSupportUserInCloud() throws IOException {
      // Should show real name.
      connectionTimelineEventHelper = new ConnectionTimelineEventHelper(cloudAirbyteSupportEmailDomain,
          currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);
      when(userPersistence.getUser(any())).thenReturn(Optional.of(externalUser));
      when(permissionHandler.isUserInstanceAdmin(any())).thenReturn(true);
      when(organizationPersistence.getOrganizationByConnectionId(any())).thenReturn(
          Optional.of(new Organization().withEmail(userEmail)));
      final UserReadInConnectionEvent userRead = connectionTimelineEventHelper.getUserReadInConnectionEvent(userId, any());
      assertEquals(false, userRead.getIsDeleted());
      assertEquals(userName, userRead.getName());
    }

  }

  @Test
  void testLogConnectionSettingsChangedEvent() {
    connectionTimelineEventHelper = new ConnectionTimelineEventHelper(Set.of(),
        currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);
    final UUID connectionId = UUID.randomUUID();
    final UUID dataplaneGroupId = UUID.randomUUID();
    final ConnectionRead originalConnectionRead = new ConnectionRead()
        .connectionId(connectionId)
        .name("old name")
        .prefix("old prefix")
        .notifySchemaChanges(false)
        .dataplaneGroupId(dataplaneGroupId)
        .notifySchemaChangesByEmail(false);
    final ConnectionUpdate patch = new ConnectionUpdate()
        .connectionId(connectionId)
        .name("new name")
        .prefix("new prefix")
        .dataplaneGroupId(dataplaneGroupId)
        .notifySchemaChanges(true);
    final Map<String, Map<String, Object>> expectedPatches = new HashMap<>();
    expectedPatches.put("name", Map.of("from", "old name", "to", "new name"));
    expectedPatches.put("prefix", Map.of("from", "old prefix", "to", "new prefix"));
    expectedPatches.put("notifySchemaChanges", Map.of("from", false, "to", true));
    connectionTimelineEventHelper.logConnectionSettingsChangedEventInConnectionTimeline(connectionId, originalConnectionRead, patch, null, true);
    ArgumentCaptor<ConnectionSettingsChangedEvent> eventCaptor = ArgumentCaptor.forClass(ConnectionSettingsChangedEvent.class);
    verify(connectionTimelineEventService).writeEvent(eq(connectionId), eventCaptor.capture(), isNull());
    ConnectionSettingsChangedEvent capturedEvent = eventCaptor.getValue();

    assertNotNull(capturedEvent);
    assertEquals(expectedPatches, capturedEvent.getPatches());
    assertNull(capturedEvent.getUpdateReason());
    assertEquals(Type.CONNECTION_SETTINGS_UPDATE, capturedEvent.getEventType());

  }

  @Test
  void testLogSchemaChangeAutoPropagationEvent() {
    connectionTimelineEventHelper = new ConnectionTimelineEventHelper(Set.of(),
        currentUserService, organizationPersistence, permissionHandler, userPersistence, connectionTimelineEventService);
    final UUID connectionId = UUID.randomUUID();
    final CatalogDiff diff = new CatalogDiff().addTransformsItem(new StreamTransform().transformType(TransformTypeEnum.ADD_STREAM));

    connectionTimelineEventHelper.logSchemaChangeAutoPropagationEventInConnectionTimeline(connectionId, diff);
    ArgumentCaptor<SchemaChangeAutoPropagationEvent> eventCaptor = ArgumentCaptor.forClass(SchemaChangeAutoPropagationEvent.class);
    verify(connectionTimelineEventService).writeEvent(eq(connectionId), eventCaptor.capture(), isNull());
    SchemaChangeAutoPropagationEvent capturedEvent = eventCaptor.getValue();

    assertNotNull(capturedEvent);
    assertEquals(diff, capturedEvent.getCatalogDiff());
    assertEquals(ConnectionAutoUpdatedReason.SCHEMA_CHANGE_AUTO_PROPAGATE.name(), capturedEvent.getUpdateReason());
    assertEquals(Type.SCHEMA_UPDATE, capturedEvent.getEventType());
  }

}
