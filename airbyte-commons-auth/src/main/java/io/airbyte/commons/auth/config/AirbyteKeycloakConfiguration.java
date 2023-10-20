/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * This class bundles all internal Keycloak configuration into a convenient singleton that can be
 * consumed by multiple Micronaut applications. Each application still needs to define each property
 * in application.yml. TODO figure out how to avoid redundant configs in each consuming
 * application.yml
 */
@ConfigurationProperties("airbyte.keycloak")
@Getter
@Setter
@Slf4j
@ToString
public class AirbyteKeycloakConfiguration {

  String protocol;
  String host;
  String basePath;
  String airbyteRealm;
  String realm;
  String clientId;
  String redirectUri;
  String webClientId;
  String accountClientId;
  String username;
  String password;
  Boolean resetRealm;

  public String getKeycloakUserInfoEndpoint() {
    final String hostWithoutTrailingSlash = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    final String basePathWithLeadingSlash = basePath.startsWith("/") ? basePath : "/" + basePath;
    final String keycloakUserInfoURI = "/protocol/openid-connect/userinfo";
    return protocol + "://" + hostWithoutTrailingSlash + basePathWithLeadingSlash + "/realms/" + airbyteRealm + keycloakUserInfoURI;
  }

}
