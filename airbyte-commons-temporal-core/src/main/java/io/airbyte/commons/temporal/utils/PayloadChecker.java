/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.utils;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.exception.SizeLimitException;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide validation to detect temporal failures earlier.
 * <p>
 * For example, when an activity returns a result that exceeds temporal payload limit, we may report
 * the activity as a success while it may fail further down in the temporal pipeline. The downside
 * is that having this fail in temporal means that we are mistakenly reporting the activity as
 * successful.
 */
@SuppressWarnings("PMD.UseVarargs")
public class PayloadChecker {

  private final MetricClient metricClient;

  private static final Logger log = LoggerFactory.getLogger(PayloadChecker.class);

  public static final int MAX_PAYLOAD_SIZE_BYTES = 4 * 1024 * 1024;

  public PayloadChecker(final MetricClient metricClient) {
    this.metricClient = metricClient;
  }

  /**
   * Validate the payload size fits within temporal message size limits.
   *
   * @param data to validate
   * @param <T> type of data
   * @return data if size is valid
   * @throws SizeLimitException if payload size exceeds temporal limits.
   */
  public <T> T validatePayloadSize(final T data) {
    return validatePayloadSize(data, new MetricAttribute[0]);
  }

  /**
   * Validate the payload size fits within temporal message size limits.
   *
   * @param data to validate
   * @param <T> type of data
   * @param attrs for metric reporting
   * @return data if size is valid
   * @throws SizeLimitException if payload size exceeds temporal limits.
   */
  public <T> T validatePayloadSize(final T data, final MetricAttribute[] attrs) {
    final String serializedData = Jsons.serialize(data);
    if (serializedData.length() > MAX_PAYLOAD_SIZE_BYTES) {
      emitInspectionLog(data);
      metricClient.count(OssMetricsRegistry.PAYLOAD_SIZE_EXCEEDED, 1, attrs);
      throw new SizeLimitException(String.format("Complete result exceeds size limit (%s of %s)", serializedData.length(), MAX_PAYLOAD_SIZE_BYTES));
    }
    return data;
  }

  private <T> void emitInspectionLog(final T data) {
    final JsonNode jsonData = Jsons.jsonNode(data);
    final Map<String, Integer> inspectionMap = new HashMap<>();
    for (Iterator<String> it = jsonData.fieldNames(); it.hasNext();) {
      var fieldName = it.next();
      inspectionMap.put(fieldName, Jsons.serialize(jsonData.get(fieldName)).length());
    }
    log.info("PayloadSize exceeded for object: {}", Jsons.serialize(inspectionMap));
  }

}
