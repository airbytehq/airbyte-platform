/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.constants.AlwaysAllowedHosts;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;

/**
 * This class takes values from a connector's configuration and uses it to fill in template-string
 * values. It substitutes strings with ${} access, e.g. "The ${animal} jumped over the ${target}"
 * with {animal: fox, target: fence}
 */
@SuppressWarnings("PMD.AvoidReassigningParameters")
public class ConfigReplacer {

  private final Logger logger;
  private final AlwaysAllowedHosts alwaysAllowedHosts = new AlwaysAllowedHosts();

  public ConfigReplacer(final Logger logger) {
    this.logger = logger;
  }

  /**
   * Note: This method does not interact with the secret manager. It is currently expected that all
   * replacement values are not secret (e.g. host vs password). This also assumed that the JSON config
   * for a connector has a single depth.
   */
  public AllowedHosts getAllowedHosts(final AllowedHosts allowedHosts, final JsonNode config) throws IOException {
    if (allowedHosts == null || allowedHosts.getHosts() == null) {
      return null;
    }

    final List<String> resolvedHosts = new ArrayList<>();
    final Map<String, String> valuesMap = new HashMap<>();
    final JsonParser jsonParser = config.traverse();

    final List<String> prefixes = new ArrayList<>();
    while (!jsonParser.isClosed()) {
      final JsonToken type = jsonParser.nextToken();
      if (type == JsonToken.FIELD_NAME) {
        final String key = jsonParser.getCurrentName();
        // the interface for allowedHosts is dot notation, e.g. `"${tunnel_method.tunnel_host}"`
        final String fullKey = (prefixes.isEmpty() ? "" : String.join(".", prefixes) + ".") + key;
        // the search path for JSON nodes is slash notation, e.g. `"/tunnel_method/tunnel_host"`
        final String lookupKey = "/" + (prefixes.isEmpty() ? "" : String.join("/", prefixes) + "/") + key;

        String value = config.at(lookupKey).textValue();
        if (value == null) {
          final Number numberValue = config.at(lookupKey).numberValue();
          if (numberValue != null) {
            value = numberValue.toString();
          }
        }

        if (value != null) {
          valuesMap.put(fullKey, sanitize(value));
        }
      } else if (type == JsonToken.START_OBJECT) {
        if (jsonParser.getCurrentName() != null) {
          prefixes.add(jsonParser.getCurrentName());
        }
      } else if (type == JsonToken.END_OBJECT) {
        if (!prefixes.isEmpty()) {
          prefixes.remove(prefixes.size() - 1);
        }
      }
    }

    final StringSubstitutor sub = new StringSubstitutor(valuesMap);
    final List<String> hosts = allowedHosts.getHosts();
    for (final String host : hosts) {
      final String replacedString = sub.replace(host);
      if (!replacedString.contains("${")) {
        resolvedHosts.add(replacedString);
      }
    }

    if (resolvedHosts.isEmpty() && !hosts.isEmpty()) {
      this.logger.error(
          "All allowedHosts values are un-replaced.  Check this connector's configuration or actor definition - " + allowedHosts.getHosts());
    }

    resolvedHosts.addAll(alwaysAllowedHosts.getHosts());

    final AllowedHosts resolvedAllowedHosts = new AllowedHosts();
    resolvedAllowedHosts.setHosts(resolvedHosts);
    return resolvedAllowedHosts;
  }

  public String sanitize(String s) {
    try {
      final String withProtocol = s.contains("://") ? s : "x://" + s;
      final URI uri = new URI(withProtocol);
      return uri.toURL().getHost();
    } catch (MalformedURLException | URISyntaxException e) {
      // some hosts will be provided from the connector config with a protocol, like ftp://site.com or
      // mongodb+srv://cluster0.abcd1.mongodb.net
      String[] parts = s.split("://");
      s = parts.length > 1 ? parts[1] : parts[0];

      // some hosts might have a trailing path. We only want the first chunk in all cases (e.g.
      // http://site.com/path/foo/bar)
      parts = s.split("/");
      s = parts[0];

      // some hosts will have a username or password, like https://user:passowrd@site.com
      parts = s.split("@");
      s = parts.length > 1 ? parts[1] : parts[0];

      // remove slashes - we only want hostnames, not paths
      s = s.replace("/", "");
      return s;
    }
  }

}
