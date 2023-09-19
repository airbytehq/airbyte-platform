/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.utils;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.exception.SizeLimitException;

/**
 * Provide validation to detect temporal failures earlier.
 * <p>
 * For example, when an activity returns a result that exceeds temporal payload limit, we may report
 * the activity as a success while it may fail further down in the temporal pipeline. The downside
 * is that having this fail in temporal means that we are mistakenly reporting the activity as
 * successful.
 */
public class PayloadChecker {

  public static final int MAX_PAYLOAD_SIZE_BYTES = 4 * 1024 * 1024;

  /**
   * Validate the payload size fits within temporal message size limits.
   *
   * @param data to validate
   * @param <T> type of data
   * @return data if size is valid
   * @throws SizeLimitException if payload size exceeds temporal limits.
   */
  public static <T> T validatePayloadSize(final T data) {
    final String serializedData = Jsons.serialize(data);
    if (serializedData.length() > MAX_PAYLOAD_SIZE_BYTES) {
      throw new SizeLimitException(String.format("Complete result exceeds size limit (%s of %s)", serializedData.length(), MAX_PAYLOAD_SIZE_BYTES));
    }
    return data;
  }

}
