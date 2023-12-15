/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncResultRead;
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody;
import io.airbyte.api.model.generated.JobStatus;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.server.handlers.api_domain_mapping.StreamStatusesMapper;
import io.airbyte.server.repositories.StreamStatusesRepository;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
public class StreamStatusesHandler {

  final StreamStatusesRepository repo;
  final StreamStatusesMapper mapper;

  public StreamStatusesHandler(final StreamStatusesRepository repo, final StreamStatusesMapper mapper) {
    this.repo = repo;
    this.mapper = mapper;
  }

  public StreamStatusRead createStreamStatus(final StreamStatusCreateRequestBody req) {
    final var model = mapper.map(req);

    final var saved = repo.save(model);

    return mapper.map(saved);
  }

  public StreamStatusRead updateStreamStatus(final StreamStatusUpdateRequestBody req) {
    final var model = mapper.map(req);

    final var saved = repo.update(model);

    return mapper.map(saved);
  }

  public StreamStatusReadList listStreamStatus(final StreamStatusListRequestBody req) {
    final var filters = mapper.map(req);

    final var page = repo.findAllFiltered(filters);

    final var apiList = page.getContent()
        .stream()
        .map(mapper::map)
        .toList();

    return new StreamStatusReadList().streamStatuses(apiList);
  }

  public StreamStatusReadList listStreamStatusPerRunState(final ConnectionIdRequestBody req) {
    final var apiList = repo.findAllPerRunStateByConnectionId(req.getConnectionId())
        .stream()
        .map(mapper::map)
        .toList();

    return new StreamStatusReadList().streamStatuses(apiList);
  }

  public ConnectionSyncResultRead mapStreamStatusToSyncReadResult(final StreamStatusRead streamStatus, final ZoneId timezone) {
    final JobStatus jobStatus = streamStatus.getRunState() == StreamStatusRunState.COMPLETE ? JobStatus.SUCCEEDED
        : streamStatus.getIncompleteRunCause() == StreamStatusIncompleteRunCause.CANCELED ? JobStatus.CANCELLED : JobStatus.FAILED;

    final ConnectionSyncResultRead result = new ConnectionSyncResultRead();
    final Instant instant = Instant.ofEpochMilli(streamStatus.getTransitionedAt());
    final ZonedDateTime zonedDateTime = instant.atZone(timezone);
    // Setting the timestamp to the start of the day in the user's timezone in epoch seconds
    result.setTimestamp(zonedDateTime.toLocalDate().atStartOfDay(timezone).toEpochSecond());
    result.setStatus(jobStatus);
    result.setStreamName(streamStatus.getStreamName());
    result.setStreamNamespace(streamStatus.getStreamNamespace());
    return result;
  }

  public List<ConnectionSyncResultRead> getConnectionUptimeHistory(final ConnectionUptimeHistoryRequestBody req) {
    final ZoneId timezone = ZoneId.of(req.getTimezone());
    final OffsetDateTime thirtyDaysAgoInUTC = ZonedDateTime.now(timezone)
        .minusDays(30)
        .toLocalDate()
        .atStartOfDay(ZoneId.of(req.getTimezone()))
        .toOffsetDateTime();

    final var streamStatuses = repo.findLatestStatusPerStreamByConnectionIdAndDayAfterTimestamp(req.getConnectionId(),
        thirtyDaysAgoInUTC, req.getTimezone())
        .stream()
        .map(mapper::map)
        .toList();

    final List<ConnectionSyncResultRead> syncReadResults = streamStatuses.stream()
        .map(status -> mapStreamStatusToSyncReadResult(status, timezone))
        .toList();

    return syncReadResults.stream()
        .sorted(Comparator
            .comparing((ConnectionSyncResultRead r) -> LocalDate.ofInstant(Instant.ofEpochSecond(r.getTimestamp()), timezone))
            .thenComparing(ConnectionSyncResultRead::getStreamNamespace, Comparator.nullsFirst(String::compareTo))
            .thenComparing(ConnectionSyncResultRead::getStreamName))
        .toList();
  }

}
