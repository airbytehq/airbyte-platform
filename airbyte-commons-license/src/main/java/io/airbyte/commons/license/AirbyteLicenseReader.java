/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
@RequiresAirbyteProEnabled
public class AirbyteLicenseReader {

  private static final AirbyteLicense INVALID_LICENSE = new AirbyteLicense(AirbyteLicense.LicenseType.INVALID,
      Optional.empty(),
      Optional.empty(),
      Optional.empty());
  public static final int EXPECTED_FRAGMENTS = 3;

  public record LicenseJwt(
                           AirbyteLicense.LicenseType license,
                           Optional<Integer> maxNodes,
                           Optional<Integer> maxEditors,
                           Long exp) {}

  private final String licenceKey;

  public AirbyteLicenseReader(@Named("licenseKey") final String licenceKey) {
    this.licenceKey = licenceKey;
  }

  public AirbyteLicense extractLicense() {
    final String[] fragments = licenceKey.split("\\.");
    if (fragments.length != EXPECTED_FRAGMENTS) {
      return INVALID_LICENSE;
    }
    final String body = fragments[1];
    final String jsonContent = new String(Base64.getDecoder().decode(body), Charset.defaultCharset());
    final LicenseJwt jwt = Jsons.deserialize(jsonContent, LicenseJwt.class);
    if (jwt.license != null && jwt.exp != null) {
      return new AirbyteLicense(jwt.license,
          Optional.of(Date.from(Instant.ofEpochMilli(jwt.exp))),
          jwt.maxNodes,
          jwt.maxEditors);
    }
    return INVALID_LICENSE;
  }

}
