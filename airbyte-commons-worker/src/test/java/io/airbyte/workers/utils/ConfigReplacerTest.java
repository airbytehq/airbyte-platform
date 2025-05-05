/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.converters.AlwaysAllowedHosts;
import io.airbyte.commons.converters.ConfigReplacer;
import io.airbyte.config.AllowedHosts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "PMD.ExceptionAsFlowControl"})
class ConfigReplacerTest {

  final ConfigReplacer replacer = new ConfigReplacer();
  final ObjectMapper mapper = new ObjectMapper();

  @Test
  @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
  void getAllowedHostsGeneralTest() throws IOException {
    final AllowedHosts allowedHosts = new AllowedHosts();
    final List<String> hosts = new ArrayList<>();
    hosts.add("localhost");
    hosts.add("static-site.com");
    hosts.add("${host}");
    hosts.add("${host_with_extras}");
    hosts.add("${number}");
    hosts.add("${subdomain}.vendor.com");
    hosts.add("${tunnel_method.tunnel_host}");
    allowedHosts.setHosts(hosts);

    final List<String> expected = new ArrayList<>();
    expected.add("localhost");
    expected.add("static-site.com");
    expected.add("foo.com");
    expected.add("protected-site.com");
    expected.add("123");
    expected.add("account.vendor.com");
    expected.add("1.2.3.4");
    expected.addAll(AlwaysAllowedHosts.getHosts());

    final String configJson =
        "{\"host\": \"foo.com\", "
            + "\"host_with_extras\": \"ftp://user:password@protected-site.com/some-route\", "
            + "\"number\": 123, "
            + "\"subdomain\": \"account\", "
            + "\"password\": \"abc123\", "
            + "\"tunnel_method\": {\"tunnel_host\": \"1.2.3.4\"}}";
    final JsonNode config = mapper.readValue(configJson, JsonNode.class);
    final AllowedHosts response = replacer.getAllowedHosts(allowedHosts, config);

    assertThat(response.getHosts()).isEqualTo(expected);
  }

  @Test
  void getAllowedHostsNestingTest() throws IOException {
    final AllowedHosts allowedHosts = new AllowedHosts();
    final List<String> hosts = new ArrayList();
    hosts.add("value-${a.b.c.d}");
    allowedHosts.setHosts(hosts);

    final List<String> expected = new ArrayList<>();
    expected.add("value-100");
    expected.addAll(AlwaysAllowedHosts.getHosts());

    final String configJson = "{\"a\": {\"b\": {\"c\": {\"d\": 100 }}}, \"array\": [1,2,3]}";
    final JsonNode config = mapper.readValue(configJson, JsonNode.class);
    final AllowedHosts response = replacer.getAllowedHosts(allowedHosts, config);

    assertThat(response.getHosts()).isEqualTo(expected);
  }

  @Test
  void ensureEmptyArrayIncludesAlwaysAllowedHosts() throws IOException {
    final AllowedHosts allowedHosts = new AllowedHosts();
    allowedHosts.setHosts(new ArrayList());

    final List<String> expected = new ArrayList<>();
    expected.addAll(AlwaysAllowedHosts.getHosts());

    final String configJson = "{}";
    final JsonNode config = mapper.readValue(configJson, JsonNode.class);
    final AllowedHosts response = replacer.getAllowedHosts(allowedHosts, config);

    assertThat(response.getHosts()).isEqualTo(expected);
    assertThat(response.getHosts()).contains("*.datadoghq.com");
  }

  @Test
  void nullAllowedHostsRemainsNull() throws IOException {
    final String configJson = "{}";
    final JsonNode config = mapper.readValue(configJson, JsonNode.class);
    final AllowedHosts response = replacer.getAllowedHosts(null, config);

    assertThat(response).isEqualTo(null);
  }

  @Test
  void alwaysAllowedHostsListIsImmutable() {
    final List<String> hosts = AlwaysAllowedHosts.getHosts();

    try {
      hosts.add("foo.com");
      throw new IOException("should not get here");
    } catch (final Exception e) {
      assertThat(e).isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  void sanitization() {
    assertThat(replacer.sanitize("basic.com")).isEqualTo("basic.com");
    assertThat(replacer.sanitize("http://basic.com")).isEqualTo("basic.com");
    assertThat(replacer.sanitize("http://user@basic.com")).isEqualTo("basic.com");
    assertThat(replacer.sanitize("http://user:password@basic.com")).isEqualTo("basic.com");
    assertThat(replacer.sanitize("http://user:password@basic.com/some/path")).isEqualTo("basic.com");
    assertThat(replacer.sanitize("mongo+srv://user:password@basic.com/some/path")).isEqualTo("basic.com");
  }

}
