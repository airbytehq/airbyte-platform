/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers;

import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.AuthSpecification;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConnectorSpecificationHelpers {

  public static ConnectorSpecification generateConnectorSpecification() throws IOException {

    final Path path = Paths.get(ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestSpecification.json").getPath());

    try {
      return new ConnectorSpecification()
          .withDocumentationUrl(new URI("https://airbyte.io"))
          .withConnectionSpecification(Jsons.deserialize(Files.readString(path)))
          .withSupportsDBT(false)
          .withSupportsNormalization(false);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static ConnectorSpecification generateAdvancedAuthConnectorSpecification() throws IOException {
    final Path specificationPath =
        Paths.get(ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestOAuthSpecification.json").getPath());
    final Path advancedAuthPath = Paths.get(
        ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestAdvancedAuth.json").getPath());
    AdvancedAuth advancedAuth = Jsons.deserialize(Files.readString(advancedAuthPath), AdvancedAuth.class);

    try {
      return new ConnectorSpecification()
          .withDocumentationUrl(new URI("https://airbyte.io"))
          .withConnectionSpecification(Jsons.deserialize(Files.readString(specificationPath)))
          .withAdvancedAuth(advancedAuth)
          .withSupportsDBT(false)
          .withSupportsNormalization(false);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
  public static ConnectorSpecification generateAuthSpecificationConnectorSpecification() throws IOException {
    final Path specificationPath =
        Paths.get(ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestOAuthSpecification.json").getPath());
    final Path authSpecificationPath = Paths.get(
        ConnectorSpecificationHelpers.class.getClassLoader().getResource("json/TestAuthSpecification.json").getPath());
    AuthSpecification authSpecification = Jsons.deserialize(Files.readString(authSpecificationPath), AuthSpecification.class);
    System.out.println("Auth specification: " + authSpecification);

    try {
      return new ConnectorSpecification()
          .withDocumentationUrl(new URI("https://airbyte.io"))
          .withConnectionSpecification(Jsons.deserialize(Files.readString(specificationPath)))
          .withAuthSpecification(authSpecification)
          .withSupportsDBT(false)
          .withSupportsNormalization(false);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}
