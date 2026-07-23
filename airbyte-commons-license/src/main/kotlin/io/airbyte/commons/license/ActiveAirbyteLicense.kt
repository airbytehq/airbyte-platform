/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.license.AirbyteLicense.LicenseType
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.nio.charset.Charset
import java.sql.Date
import java.time.Instant
import java.util.Base64
import java.util.UUID

/**
 * Bean that contains the Airbyte License that is retrieved from the licensing server.
 */
@Singleton
@RequiresAirbyteProEnabled
class ActiveAirbyteLicense(
  @param:Value("\${airbyte.license-key}") private val licenceKey: String,
) {
  var license: AirbyteLicense = extractLicense()

  val isPro: Boolean
    get() = license.type == LicenseType.PRO

  private fun extractLicense(): AirbyteLicense {
    try {
      val fragments = licenceKey.split(".")
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
          jwt.enterpriseConnectorIds?.map { UUID.fromString(it) }?.toSet() ?: emptySet(),
          isEmbedded = jwt.isEmbedded ?: false,
        )
      }
      return INVALID_LICENSE
    } catch (_: Exception) {
      // Handle Base64 decoding errors, JSON parsing errors, and any other exceptions
      return INVALID_LICENSE
    }
  }

  companion object {
    private val INVALID_LICENSE = AirbyteLicense(type = LicenseType.INVALID)
    private const val EXPECTED_FRAGMENTS: Int = 3
  }

  private data class LicenseJwt(
    val license: LicenseType?,
    val maxNodes: Int?,
    val maxEditors: Int?,
    val enterpriseConnectorIds: List<String>?,
    val exp: Long?,
    val isEmbedded: Boolean?,
  )
}
