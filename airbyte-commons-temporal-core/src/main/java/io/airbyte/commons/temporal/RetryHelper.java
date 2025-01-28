/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;

/**
 * Helper class to provide consistent retry strategy across the temporal wrapper classes.
 */
public class RetryHelper {

  public static final int DEFAULT_MAX_ATTEMPT = 3;
  public static final int DEFAULT_BACKOFF_DELAY_IN_MILLIS = 1000;
  public static final int DEFAULT_BACKOFF_MAX_DELAY_IN_MILLIS = 10000;

  private final MetricClient metricClient;
  private final int maxAttempt;
  private final int backoffDelayInMillis;
  private final int backoffMaxDelayInMillis;

  public RetryHelper(final MetricClient metricClient,
                     final int maxAttempt,
                     final int backoffDelayInMillis,
                     final int backoffMaxDelayInMillis) {
    this.metricClient = metricClient;
    this.maxAttempt = maxAttempt;
    this.backoffDelayInMillis = backoffDelayInMillis;
    this.backoffMaxDelayInMillis = backoffMaxDelayInMillis;
  }

  /**
   * Where the magic happens.
   * <p>
   * We should only retry errors that are transient GRPC network errors.
   * <p>
   * We should only retry idempotent calls. The caller should be responsible for retrying creates to
   * avoid generating additional noise.
   */
  public <T> T withRetries(final CheckedSupplier<T> call, final String name) {
    final var retry = RetryPolicy.builder()
        .handleIf(this::shouldRetry)
        .withMaxAttempts(maxAttempt)
        .withBackoff(Duration.ofMillis(backoffDelayInMillis), Duration.ofMillis(backoffMaxDelayInMillis))
        .onRetry((a) -> metricClient.count(OssMetricsRegistry.TEMPORAL_API_TRANSIENT_ERROR_RETRY, 1,
            new MetricAttribute(MetricTags.ATTEMPT_NUMBER, String.valueOf(a.getAttemptCount())),
            new MetricAttribute(MetricTags.FAILURE_ORIGIN, name),
            new MetricAttribute(MetricTags.FAILURE_TYPE, a.getLastException().getClass().getName())))
        .build();
    return Failsafe.with(retry).get(call);
  }

  private boolean shouldRetry(final Throwable t) {
    // We are retrying Status.UNAVAILABLE because it is often sign of an unexpected connection
    // termination.
    return t instanceof StatusRuntimeException && Status.UNAVAILABLE.equals(((StatusRuntimeException) t).getStatus());
  }

}
