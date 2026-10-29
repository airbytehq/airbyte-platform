/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.net

import com.google.common.net.InetAddresses
import java.net.InetAddress

/**
 * Parses [host] as an IP literal without any DNS lookup; an IPv6 literal may be wrapped in brackets. Returns null for
 * hostnames and malformed literals, and for IPv6 zone IDs (`fe80::1%eth0`), which only mean something on the local
 * machine.
 */
fun parseIpLiteral(host: String): InetAddress? {
  val literal = if (host.startsWith("[") && host.endsWith("]")) host.substring(1, host.length - 1) else host
  if ('%' in literal || !InetAddresses.isInetAddress(literal)) {
    return null
  }
  return InetAddresses.forString(literal)
}
