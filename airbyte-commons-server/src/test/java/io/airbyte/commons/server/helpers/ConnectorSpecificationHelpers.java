/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers;

import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConnectorSpecificationHelpers {

  static final Path oAuthSpecificationPath =
      Paths.get(ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestOAuthSpecification.json").getPath());
  private static final String DOCUMENTATION_URL = "https://airbyte.io";

  public static ConnectorSpecification generateConnectorSpecification() throws IOException {

    final Path path = Paths.get(ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestSpecification.json").getPath());

    try {
      return new ConnectorSpecification()
          .withDocumentationUrl(new URI(DOCUMENTATION_URL))
          .withConnectionSpecification(Jsons.deserialize(Files.readString(path)))
          .withSupportsDBT(false)
          .withSupportsNormalization(false);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static ConnectorSpecification generateAdvancedAuthConnectorSpecification() throws IOException {
    final Path advancedAuthPath = Paths.get(
        ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestAdvancedAuth.json").getPath());
    AdvancedAuth advancedAuth = Jsons.deserialize(Files.readString(advancedAuthPath), AdvancedAuth.class);

    try {
      return new ConnectorSpecification()
          .withDocumentationUrl(new URI(DOCUMENTATION_URL))
          .withConnectionSpecification(Jsons.deserialize(Files.readString(oAuthSpecificationPath)))
          .withAdvancedAuth(advancedAuth)
          .withSupportsDBT(false)
          .withSupportsNormalization(false);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static ConnectorSpecification generateNestedAdvancedAuthConnectorSpecification() throws IOException {
    final Path advancedAuthPath = Paths.get(
        ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestAdvancedAuthNested.json").getPath());
    AdvancedAuth advancedAuth = Jsons.deserialize(Files.readString(advancedAuthPath), AdvancedAuth.class);

    try {
      return new ConnectorSpecification()
          .withDocumentationUrl(new URI(DOCUMENTATION_URL))
          .withConnectionSpecification(Jsons.deserialize(Files.readString(oAuthSpecificationPath)))
          .withAdvancedAuth(advancedAuth)
          .withSupportsDBT(false)
          .withSupportsNormalization(false);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}
