/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.utils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.converters.AlwaysAllowedHosts
import io.airbyte.commons.converters.AlwaysAllowedHosts.hosts
import io.airbyte.commons.converters.ConfigReplacer
import io.airbyte.config.AllowedHosts
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException

internal class ConfigReplacerTest {
  val replacer: ConfigReplacer = ConfigReplacer()
  val mapper: ObjectMapper = ObjectMapper()

  @Test
  @Throws(IOException::class)
  fun getAllowedHostsGeneralTest() {
    val allowedHosts = AllowedHosts()
    val hosts: MutableList<String?> = ArrayList<String?>()
    hosts.add("localhost")
    hosts.add("static-site.com")
    hosts.add("\${host}")
    hosts.add("\${host_with_extras}")
    hosts.add("\${number}")
    hosts.add("\${subdomain}.vendor.com")
    hosts.add("\${tunnel_method.tunnel_host}")
    allowedHosts.setHosts(hosts)

    val expected: MutableList<String?> = ArrayList<String?>()
    expected.add("localhost")
    expected.add("static-site.com")
    expected.add("foo.com")
    expected.add("protected-site.com")
    expected.add("123")
    expected.add("account.vendor.com")
    expected.add("1.2.3.4")
    expected.addAll(AlwaysAllowedHosts.hosts)

    val configJson =
      (
        "{\"host\": \"foo.com\", " +
          "\"host_with_extras\": \"ftp://user:password@protected-site.com/some-route\", " +
          "\"number\": 123, " +
          "\"subdomain\": \"account\", " +
          "\"password\": \"abc123\", " +
          "\"tunnel_method\": {\"tunnel_host\": \"1.2.3.4\"}}"
      )
    val config = mapper.readValue<JsonNode>(configJson, JsonNode::class.java)
    val response = replacer.getAllowedHosts(allowedHosts, config)

    Assertions.assertThat<String?>(response!!.getHosts()).isEqualTo(expected)
  }

  @Test
  @Throws(IOException::class)
  fun getAllowedHostsNestingTest() {
    val allowedHosts = AllowedHosts()
    val hosts: MutableList<String?> = ArrayList<String?>()
    hosts.add("value-\${a.b.c.d}")
    allowedHosts.setHosts(hosts)

    val expected: MutableList<String?> = ArrayList<String?>()
    expected.add("value-100")
    expected.addAll(AlwaysAllowedHosts.hosts)

    val configJson = "{\"a\": {\"b\": {\"c\": {\"d\": 100 }}}, \"array\": [1,2,3]}"
    val config = mapper.readValue<JsonNode>(configJson, JsonNode::class.java)
    val response = replacer.getAllowedHosts(allowedHosts, config)

    Assertions.assertThat<String?>(response!!.getHosts()).isEqualTo(expected)
  }

  @Test
  @Throws(IOException::class)
  fun ensureEmptyArrayIncludesAlwaysAllowedHosts() {
    val allowedHosts = AllowedHosts()
    allowedHosts.setHosts(ArrayList<String?>())

    val expected: MutableList<String?> = ArrayList<String?>()
    expected.addAll(hosts)

    val configJson = "{}"
    val config = mapper.readValue<JsonNode>(configJson, JsonNode::class.java)
    val response = replacer.getAllowedHosts(allowedHosts, config)

    Assertions.assertThat<String?>(response!!.getHosts()).isEqualTo(expected)
    Assertions.assertThat<String?>(response.getHosts()).contains("*.datadoghq.com")
  }

  @Test
  @Throws(IOException::class)
  fun nullAllowedHostsRemainsNull() {
    val configJson = "{}"
    val config = mapper.readValue<JsonNode>(configJson, JsonNode::class.java)
    val response = replacer.getAllowedHosts(null, config)

    Assertions.assertThat<AllowedHosts?>(response).isEqualTo(null)
  }

  @Test
  fun alwaysAllowedHostsListIsImmutable() {
    val hosts: MutableList<String?> = AlwaysAllowedHosts.hosts as MutableList<String?>

    try {
      hosts.add("foo.com")
      throw IOException("should not get here")
    } catch (e: Exception) {
      Assertions.assertThat<Exception?>(e).isInstanceOf(UnsupportedOperationException::class.java)
    }
  }

  @Test
  fun sanitization() {
    with(ConfigReplacer()) {
      Assertions.assertThat("basic.com".sanitize()).isEqualTo("basic.com")
    }
    with(ConfigReplacer()) {
      Assertions.assertThat("http://basic.com".sanitize()).isEqualTo("basic.com")
    }
    with(ConfigReplacer()) {
      Assertions.assertThat("http://user@basic.com".sanitize()).isEqualTo("basic.com")
    }
    with(ConfigReplacer()) {
      Assertions.assertThat("http://user:password@basic.com".sanitize()).isEqualTo("basic.com")
    }
    with(ConfigReplacer()) {
      Assertions.assertThat("http://user:password@basic.com/some/path".sanitize()).isEqualTo("basic.com")
    }
    with(ConfigReplacer()) {
      Assertions.assertThat("mongo+srv://user:password@basic.com/some/path".sanitize()).isEqualTo("basic.com")
    }
  }
}
