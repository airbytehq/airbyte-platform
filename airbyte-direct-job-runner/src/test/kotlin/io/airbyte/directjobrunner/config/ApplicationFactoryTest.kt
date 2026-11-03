/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetAddress

class ApplicationFactoryTest {
  private val factory = ApplicationFactory()

  private fun ip(value: String) = InetAddress.getByName(value)

  @ParameterizedTest
  @ValueSource(strings = ["10.0.0.5", "127.0.0.1", "169.254.169.254", "192.168.1.1", "93.184.216.34"])
  fun `blocks nothing when internal address blocking is off`(address: String) {
    val matcher = factory.blockedAddressMatcher(ConnectConfig(blockInternalAddresses = false))

    assertFalse(matcher.isBlocked(ip(address)))
  }

  @Test
  fun `ignores extra blocked CIDRs when internal address blocking is off`() {
    val matcher = factory.blockedAddressMatcher(ConnectConfig(blockInternalAddresses = false, blockedCidrs = listOf("203.0.113.0/24")))

    assertFalse(matcher.isBlocked(ip("203.0.113.10")))
  }

  @ParameterizedTest
  @ValueSource(strings = ["10.0.0.5", "127.0.0.1", "169.254.169.254", "192.168.1.1"])
  fun `blocks the default internal ranges when internal address blocking is on`(address: String) {
    val matcher = factory.blockedAddressMatcher(ConnectConfig(blockInternalAddresses = true))

    assertTrue(matcher.isBlocked(ip(address)))
  }

  @Test
  fun `blocks extra CIDRs on top of the defaults when internal address blocking is on`() {
    val matcher = factory.blockedAddressMatcher(ConnectConfig(blockInternalAddresses = true, blockedCidrs = listOf(" 203.0.113.0/24 ", "")))

    assertTrue(matcher.isBlocked(ip("203.0.113.10")))
    assertTrue(matcher.isBlocked(ip("10.0.0.5")))
    assertFalse(matcher.isBlocked(ip("93.184.216.34")))
  }

  @Test
  fun `defaults to no blocking`() {
    assertFalse(ConnectConfig().blockInternalAddresses)
  }
}
