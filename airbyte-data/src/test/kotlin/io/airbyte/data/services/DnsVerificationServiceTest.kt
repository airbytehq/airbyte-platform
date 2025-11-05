/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class DnsVerificationServiceTest {
  private val service = DnsVerificationService()

  @Nested
  inner class CheckDomainVerification {
    @Test
    fun `should return NotFound when DNS records do not exist`() {
      val result =
        service.checkDomainVerification(
          dnsRecordName = "_airbyte-verification-nonexistent.example.com",
          expectedValue = "airbyte-domain-verification=abc123",
        )

      result shouldBe DnsVerificationResult.NotFound
    }

    @Test
    fun `should handle invalid domain names gracefully`() {
      val result =
        service.checkDomainVerification(
          dnsRecordName = "not a valid domain name!@#$%",
          expectedValue = "airbyte-domain-verification=abc123",
        )

      result shouldBe DnsVerificationResult.NotFound
    }
  }

  @Nested
  inner class LookupTxtRecords {
    @Test
    fun `should return empty list when domain has no TXT records`() {
      val records = service.lookupTxtRecords("_airbyte-nonexistent.example.com")

      records shouldBe emptyList()
    }

    @Test
    fun `should handle invalid domain names gracefully`() {
      val records = service.lookupTxtRecords("not a valid domain!")

      records shouldBe emptyList()
    }
  }

  @Nested
  inner class Normalization {
    @Test
    fun `should match records with different quote styles`() {
      val baseRecord = Pair("airbyte-domain-verification", "abc123")

      // All of these variations should match the base record
      // Note: Attribute names are case-insensitive, but values are case-sensitive per RFC 1464
      val variations =
        listOf(
          // Quoted record
          "\"airbyte-domain-verification=abc123\"",
          // Extra whitespace
          " airbyte-domain-verification=abc123 ",
          // Case variations in ATTRIBUTE NAME (case-insensitive)
          "AIRBYTE-DOMAIN-VERIFICATION=abc123",
          "Airbyte-Domain-Verification=abc123",
          // Combined: quotes + whitespace + case
          "\" AIRBYTE-DOMAIN-VERIFICATION=abc123 \"",
          // Spaces around equals
          "airbyte-domain-verification = abc123",
          // All combined
          "\" Airbyte-Domain-Verification = abc123 \"",
        )

      variations.forEach { variation ->
        val parsed = service.parseRfc1464Record(variation)
        (parsed != null) shouldBe true
        service.recordsMatch(baseRecord, parsed!!) shouldBe true
      }
    }

    @Test
    fun `should not match records with different values`() {
      val record1 = Pair("airbyte-domain-verification", "token123")
      val record2 = Pair("airbyte-domain-verification", "differentToken")

      service.recordsMatch(record1, record2) shouldBe false
    }

    @Test
    fun `should not match records with different attributes`() {
      val record1 = Pair("airbyte-domain-verification", "token123")
      val record2 = Pair("different-attribute", "token123")

      service.recordsMatch(record1, record2) shouldBe false
    }
  }

  @Nested
  inner class Rfc1464Compliance {
    @Test
    fun `should parse valid attribute=value format`() {
      val result = service.parseRfc1464Record("airbyte-domain-verification=abc123")

      result shouldBe Pair("airbyte-domain-verification", "abc123")
    }

    @Test
    fun `should handle spaces around equals sign`() {
      val result = service.parseRfc1464Record("airbyte-domain-verification = abc123")

      result shouldBe Pair("airbyte-domain-verification ", " abc123")
      // Normalization will trim these spaces during comparison
    }

    @Test
    fun `should match records with spaces around equals after normalization`() {
      val expected = Pair("airbyte-domain-verification", "abc123")
      val actual = Pair("airbyte-domain-verification ", " abc123")

      service.recordsMatch(expected, actual) shouldBe true
    }

    @Test
    fun `should handle quoted equals in attribute name`() {
      val result = service.parseRfc1464Record("attr`=name=value123")

      result shouldBe Pair("attr`=name", "value123")
    }

    @Test
    fun `should ignore records without equals sign`() {
      val result = service.parseRfc1464Record("no-equals-sign-here")

      result shouldBe null
    }

    @Test
    fun `should ignore records starting with equals`() {
      val result = service.parseRfc1464Record("=invalid")

      result shouldBe null
    }

    @Test
    fun `should match records with case-insensitive attribute names`() {
      val expected = Pair("airbyte-domain-verification", "abc123")
      val actual = Pair("AIRBYTE-DOMAIN-VERIFICATION", "abc123")

      service.recordsMatch(expected, actual) shouldBe true
    }

    @Test
    fun `should not match records with different value case`() {
      // Per RFC 1464: only attribute names are case-insensitive, values are case-sensitive
      val expected = Pair("airbyte-domain-verification", "abc123")
      val actual = Pair("airbyte-domain-verification", "ABC123")

      service.recordsMatch(expected, actual) shouldBe false
    }

    @Test
    fun `should remove surrounding quotes from record`() {
      val result = service.parseRfc1464Record("\"airbyte-domain-verification=abc123\"")

      result shouldBe Pair("airbyte-domain-verification", "abc123")
    }

    @Test
    fun `should handle backtick escaping in attribute name`() {
      // Per RFC 1464: backtick escapes the NEXT character
      // `= means a literal equals sign in the attribute name
      val expected = Pair("attr=name", "value")
      val actual = Pair("attr`=name", "value")

      service.recordsMatch(expected, actual) shouldBe true
    }

    @Test
    fun `should handle multiple equals signs (takes first unquoted)`() {
      val result = service.parseRfc1464Record("attribute=value=with=more=equals")

      result shouldBe Pair("attribute", "value=with=more=equals")
    }
  }
}
