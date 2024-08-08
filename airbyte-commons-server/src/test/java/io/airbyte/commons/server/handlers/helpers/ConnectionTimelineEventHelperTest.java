/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.persistence.job.JobPersistence;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectionTimelineEventHelperTest {

  private CurrentUserService currentUserService;
  private ConnectionTimelineEventService connectionTimelineEventService;
  private ConnectionTimelineEventHelper connectionTimelineEventHelper;
  private static final UUID CONNECTION_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    currentUserService = mock(CurrentUserService.class);
    connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    connectionTimelineEventHelper = new ConnectionTimelineEventHelper(connectionTimelineEventService, currentUserService);
  }

  @Test
  void testGetLoadedStats() {

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
        new Job(100L, SYNC, CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.SUCCEEDED, 0L, 0L, 0L);

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

}
