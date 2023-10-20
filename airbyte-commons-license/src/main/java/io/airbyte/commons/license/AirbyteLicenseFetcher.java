/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.license.AirbyteLicense.LicenseType;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Singleton responsible for fetching an Airbyte License. Always returns at least an INVALID license
 * so that application startup is never blocked due to a license retrieval issue.
 */
@Singleton
@Slf4j
@Requires(property = "airbyte.license-key")
public class AirbyteLicenseFetcher {

  private static final String LICENSE_URL = "https://oss.airbyte.com/license";
  private static final String LICENSE_KEY_REQUEST_BODY_KEY = "licenseKey";

  private final String licenceKey;
  private final HttpClient httpClient;

  public AirbyteLicenseFetcher(@Value("${airbyte.license-key}") final String licenceKey, final HttpClient httpClient) {
    this.licenceKey = licenceKey;
    this.httpClient = httpClient;
  }

  public AirbyteLicense fetchLicense() {
    final JsonNode requestBody = Jsons.jsonNode(ImmutableMap.builder()
        .put(LICENSE_KEY_REQUEST_BODY_KEY, licenceKey)
        .build());

    final HttpRequest<JsonNode> request = HttpRequest.POST(LICENSE_URL, requestBody)
        .header("Content-Type", "application/json");

    final String responseBody;
    try {
      responseBody = httpClient.toBlocking().exchange(request, String.class).body();
    } catch (final Exception e) {
      log.warn("Returning INVALID license due to an error while attempting to retrieve license from server.", e);
      return new AirbyteLicense(LicenseType.INVALID);
    }

    try {
      log.info("Received license response body {}", responseBody);
      return Jsons.deserialize(responseBody, AirbyteLicense.class);
    } catch (Exception e) {
      log.warn("Returning INVALID license due to a deserialization failure for response: {}", responseBody, e);
      return new AirbyteLicense(LicenseType.INVALID);
    }
  }

}
