/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.license.AirbyteLicense.LicenseType
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.charset.Charset
import java.sql.Date
import java.time.Instant
import java.util.Base64

@Singleton
@RequiresAirbyteProEnabled
class AirbyteLicenseReader(
  @param:Named("licenseKey") private val licenceKey: String,
) {
  fun extractLicense(): AirbyteLicense {
    val fragments = licenceKey.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
    if (fragments.size != EXPECTED_FRAGMENTS) {
      return INVALID_LICENSE
    }
    val body = fragments[1]
    val jsonContent = String(Base64.getDecoder().decode(body), Charset.defaultCharset())
    val jwt = Jsons.deserialize(jsonContent, LicenseJwt::class.java)
    if (jwt.license != null && jwt.exp != null) {
      return AirbyteLicense(
        jwt.license,
        Date.from(Instant.ofEpochMilli(jwt.exp)),
        jwt.maxNodes,
        jwt.maxEditors,
      )
    }
    return INVALID_LICENSE
  }
}

private val INVALID_LICENSE = AirbyteLicense(type = LicenseType.INVALID)
private const val EXPECTED_FRAGMENTS: Int = 3

private data class LicenseJwt(
  val license: LicenseType?,
  val maxNodes: Int?,
  val maxEditors: Int?,
  val exp: Long?,
)
