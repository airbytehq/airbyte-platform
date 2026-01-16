/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.annotation.InternalForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.Locale
import java.util.Properties
import javax.naming.Context
import javax.naming.NamingException
import javax.naming.directory.Attribute
import javax.naming.directory.InitialDirContext

private val logger = KotlinLogging.logger { }

/**
 * Result of DNS verification check.
 */
sealed class DnsVerificationResult {
  /** DNS record exists with the correct value */
  data object Verified : DnsVerificationResult()

  /** No DNS record found at the expected location */
  data object NotFound : DnsVerificationResult()

  /** DNS record(s) found but none match the expected value */
  data class Misconfigured(
    val foundRecords: List<String>,
  ) : DnsVerificationResult()
}

/**
 * Service for verifying domain ownership via DNS TXT records.
 *
 * Uses Java's built-in JNDI (Java Naming and Directory Interface) to perform DNS lookups
 * without requiring external dependencies.
 */
@Singleton
class DnsVerificationService {
  companion object {
    private const val DNS_PROVIDER_URL = "dns:"
    private const val TXT_RECORD_TYPE = "TXT"
    private const val DNS_TIMEOUT_MS = "5000"
    private const val DNS_RETRIES = "1"
  }

  /**
   * Verifies domain ownership by checking if the expected DNS TXT record exists.
   *
   * @param dnsRecordName The fully qualified DNS name to look up (e.g., "_airbyte-verification.example.com")
   * @param expectedValue The expected TXT record value (e.g., "airbyte-domain-verification=abc123...")
   * @return DnsVerificationResult indicating whether the record is verified, not found, or misconfigured
   */
  fun checkDomainVerification(
    dnsRecordName: String,
    expectedValue: String,
  ): DnsVerificationResult {
    return try {
      val txtRecords = lookupTxtRecords(dnsRecordName)
      val expectedParsed = parseRfc1464Record(expectedValue)

      if (expectedParsed == null) {
        logger.error { "Expected value is not in valid RFC 1464 attribute=value format: $expectedValue" }
        return DnsVerificationResult.NotFound
      }

      val found =
        txtRecords.any { record ->
          val recordParsed = parseRfc1464Record(record)
          recordParsed != null && recordsMatch(expectedParsed, recordParsed)
        }

      when {
        found -> {
          logger.info { "DNS verification successful for $dnsRecordName" }
          DnsVerificationResult.Verified
        }
        txtRecords.isEmpty() -> {
          logger.debug { "No DNS TXT records found for $dnsRecordName" }
          DnsVerificationResult.NotFound
        }
        else -> {
          val parsedRecords =
            txtRecords.mapNotNull { record ->
              parseRfc1464Record(record)?.let { (attr, value) ->
                "${normalizeAttributeName(attr)}=${normalizeValue(value)}"
              }
            }
          logger.warn {
            "DNS TXT record misconfigured for $dnsRecordName. " +
              "Expected: '${normalizeAttributeName(expectedParsed.first)}=${normalizeValue(expectedParsed.second)}', " +
              "Found: ${parsedRecords.map { "'$it'" }}"
          }
          DnsVerificationResult.Misconfigured(parsedRecords)
        }
      }
    } catch (e: Exception) {
      logger.error(e) { "DNS lookup failed for $dnsRecordName" }
      DnsVerificationResult.NotFound
    }
  }

  /**
   * Looks up all TXT records for the given DNS hostname.
   *
   * @param hostname The fully qualified DNS name to query
   * @return List of TXT record values found (could be empty if none exist or lookup fails)
   */
  @InternalForTesting
  internal fun lookupTxtRecords(hostname: String): List<String> {
    val records = mutableListOf<String>()

    try {
      val env =
        Properties().apply {
          setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
          setProperty(Context.PROVIDER_URL, DNS_PROVIDER_URL)
          setProperty("com.sun.jndi.dns.timeout.initial", DNS_TIMEOUT_MS)
          setProperty("com.sun.jndi.dns.timeout.retries", DNS_RETRIES)
        }

      var context: InitialDirContext? = null
      try {
        context = InitialDirContext(env)
        val attributes = context.getAttributes(hostname, arrayOf(TXT_RECORD_TYPE))
        val txtAttribute = attributes.get(TXT_RECORD_TYPE)

        if (txtAttribute != null) {
          records.addAll(extractRecordValues(txtAttribute))
        }
      } finally {
        context?.close()
      }

      logger.debug { "Found ${records.size} TXT records for $hostname" }
    } catch (e: NamingException) {
      // This can happen if the domain doesn't exist or has no TXT records
      logger.debug { "No TXT records found for $hostname: ${e.message}" }
    } catch (e: Exception) {
      logger.error(e) { "Unexpected error during DNS lookup for $hostname" }
    }

    return records
  }

  /**
   * Extracts string values from a DNS TXT record attribute.
   * TXT records may be returned with surrounding quotes that need to be removed.
   */
  private fun extractRecordValues(attribute: Attribute): List<String> {
    val all = attribute.all
    return generateSequence { if (all.hasMore()) all.next() else null }
      .map { it.toString() }
      .toList()
  }

  /**
   * Parses a TXT record in RFC 1464 attribute=value format.
   *
   * Per RFC 1464:
   * - Format is: attribute=value
   * - The first unquoted "=" is the delimiter
   * - Backtick (`) is used to quote special characters in the attribute name
   * - TXT records without an unquoted "=" are ignored
   * - TXT records starting with "=" (null attribute name) are ignored
   *
   * @return Pair of (attribute, value) or null if not in valid RFC 1464 format
   */
  @InternalForTesting
  internal fun parseRfc1464Record(record: String): Pair<String, String>? {
    // Remove surrounding quotes from DNS representation
    val cleanRecord = record.trim().removeSurrounding("\"")

    // Find first unquoted equals sign
    // Per RFC 1464: backtick (`) escapes the NEXT character
    var equalsIndex = -1
    var i = 0

    while (i < cleanRecord.length) {
      when (cleanRecord[i]) {
        '`' -> i++ // Skip next character (it's escaped)
        '=' -> {
          equalsIndex = i
          break
        }
      }
      i++
    }

    // Per RFC 1464: ignore records without "=" or starting with "="
    if (equalsIndex <= 0) {
      return null
    }

    val attribute = cleanRecord.take(equalsIndex)
    val value = cleanRecord.substring(equalsIndex + 1)

    return Pair(attribute, value)
  }

  /**
   * Normalizes an RFC 1464 attribute name per the specification:
   * - Remove backtick escape characters (but keep the escaped characters)
   * - Trim leading/trailing whitespace
   * - Convert to lowercase (attribute names are case-insensitive per RFC 1464)
   *
   * Uses locale-insensitive lowercasing (Locale.ROOT) to avoid issues like Turkish I/ı.
   *
   * Example: "attr`=name" becomes "attr=name"
   */
  private fun normalizeAttributeName(attribute: String): String {
    // Remove backtick escape characters
    val unescaped =
      buildString {
        var i = 0
        while (i < attribute.length) {
          if (attribute[i] == '`' && i + 1 < attribute.length) {
            // Skip the backtick, append the escaped character
            i++
            append(attribute[i])
          } else {
            append(attribute[i])
          }
          i++
        }
      }

    return unescaped.trim().lowercase(Locale.ROOT)
  }

  /**
   * Normalizes a TXT record value for comparison.
   *
   * Per RFC 1464: "All whitespace in the attribute value is returned to the requestor
   * (it is up to the application to decide if it is significant.)"
   *
   * We choose to:
   * - Trim whitespace for better UX (users may add accidental spaces)
   * - Use case-sensitive comparison for values (only attribute names are case-insensitive per RFC 1464)
   *
   * References:
   * - RFC 1035 §3.3.14: TXT record format and character-string definition
   * - RFC 1464: Attribute names are case-insensitive; values are case-sensitive (application decides)
   * - RFC 4343: DNS case insensitivity clarification (applies to domain names and attribute names)
   */
  private fun normalizeValue(value: String): String =
    value
      .trim()

  /**
   * Compares two RFC 1464 parsed records (attribute, value pairs) for equality.
   * Uses normalized comparison for both attribute and value.
   */
  @InternalForTesting
  internal fun recordsMatch(
    expected: Pair<String, String>,
    actual: Pair<String, String>,
  ): Boolean {
    val normalizedExpectedAttr = normalizeAttributeName(expected.first)
    val normalizedExpectedValue = normalizeValue(expected.second)
    val normalizedActualAttr = normalizeAttributeName(actual.first)
    val normalizedActualValue = normalizeValue(actual.second)

    return normalizedExpectedAttr == normalizedActualAttr &&
      normalizedExpectedValue == normalizedActualValue
  }
}
