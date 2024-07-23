/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delete old files that accumulate in docker.
 */
@Singleton
@Requires(notEnv = Environment.KUBERNETES)
public class WorkspaceCleaner {

  private static final Logger log = LoggerFactory.getLogger(WorkspaceCleaner.class);

  private final Path workspaceRootPath;
  private final long maxAgeFilesInDays;
  private final MetricClient metricClient;

  public static final String DEFAULT_TEMPORAL_HISTORY_RETENTION_IN_DAYS = "30";

  public WorkspaceCleaner(
                          final MetricClient metricClient,
                          @Value("${airbyte.workspace.root}") final String workspaceRoot) {
    this.workspaceRootPath = Path.of(workspaceRoot);
    // We align max file age on temporal for history consistency
    // It might make sense configure this independently in the future
    this.maxAgeFilesInDays = Integer.parseInt(
        EnvVar.TEMPORAL_HISTORY_RETENTION_IN_DAYS.fetch(DEFAULT_TEMPORAL_HISTORY_RETENTION_IN_DAYS));
    this.metricClient = metricClient;
  }

  /**
   * Delete files older than maxAgeFilesInDays from the workspace. NOTE: this is currently only
   * intended to work for docker.
   *
   * @throws IOException exception while interacting with the database
   */
  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "1d")
  public void deleteOldFiles() throws IOException {
    final Date oldestAllowed = getDateFromDaysAgo(maxAgeFilesInDays);
    log.info("Deleting files older than {} days ({})", maxAgeFilesInDays, oldestAllowed);
    metricClient.count(OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE, 1,
        new MetricAttribute(MetricTags.CRON_TYPE, "workspace_cleaner"));

    ApmTraceUtils.addTagsToTrace(Map.of("oldest_date_allowed", oldestAllowed, "max_age", maxAgeFilesInDays));

    final AtomicInteger counter = new AtomicInteger(0);

    // Check if workspace root path exists. It may not exist during tests.
    if (workspaceRootPath.toFile().exists()) {
      Files.walk(workspaceRootPath)
          .map(Path::toFile)
          .filter(f -> new AgeFileFilter(oldestAllowed).accept(f))
          .forEach(file -> {
            log.debug("Deleting file: {}", file);
            FileUtils.deleteQuietly(file);
            counter.incrementAndGet();
            final File parentDir = file.getParentFile();
            if (parentDir.isDirectory() && parentDir.listFiles().length == 0) {
              FileUtils.deleteQuietly(parentDir);
            }
          });
      log.info("deleted {} files", counter.get());
    }
  }

  private static Date getDateFromDaysAgo(final long daysAgo) {
    return Date.from(LocalDateTime.now().minusDays(daysAgo).toInstant(OffsetDateTime.now().getOffset()));
  }

}
