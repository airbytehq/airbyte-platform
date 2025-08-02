/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object ConnectorSpecificationHelpers {
  val oAuthSpecificationPath: Path =
    Paths.get(
      ConnectorSpecificationHelpers::class.java
        .getClassLoader()
        .getResource("json/TestOAuthSpecification.json")
        .getPath(),
    )
  private const val DOCUMENTATION_URL = "https://airbyte.io"

  @Throws(IOException::class)
  fun generateConnectorSpecification(): ConnectorSpecification {
    val path =
      Paths.get(
        ConnectorSpecificationHelpers::class.java
          .getClassLoader()
          .getResource("json/TestSpecification.json")
          .getPath(),
      )

    try {
      return ConnectorSpecification()
        .withDocumentationUrl(URI(DOCUMENTATION_URL))
        .withConnectionSpecification(deserialize(Files.readString(path)))
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
    } catch (e: URISyntaxException) {
      throw RuntimeException(e)
    }
  }

  @Throws(IOException::class)
  fun generateAdvancedAuthConnectorSpecification(): ConnectorSpecification? {
    val advancedAuthPath =
      Paths.get(
        ConnectorSpecificationHelpers::class.java
          .getClassLoader()
          .getResource("json/TestAdvancedAuth.json")
          .getPath(),
      )
    val advancedAuth: AdvancedAuth? = Jsons.deserialize<AdvancedAuth>(Files.readString(advancedAuthPath), AdvancedAuth::class.java)

    try {
      return ConnectorSpecification()
        .withDocumentationUrl(URI(DOCUMENTATION_URL))
        .withConnectionSpecification(deserialize(Files.readString(oAuthSpecificationPath)))
        .withAdvancedAuth(advancedAuth)
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
    } catch (e: URISyntaxException) {
      throw RuntimeException(e)
    }
  }

  @Throws(IOException::class)
  fun generateNestedAdvancedAuthConnectorSpecification(): ConnectorSpecification? {
    val advancedAuthPath =
      Paths.get(
        ConnectorSpecificationHelpers::class.java
          .getClassLoader()
          .getResource("json/TestAdvancedAuthNested.json")
          .getPath(),
      )
    val advancedAuth: AdvancedAuth? = Jsons.deserialize<AdvancedAuth>(Files.readString(advancedAuthPath), AdvancedAuth::class.java)

    try {
      return ConnectorSpecification()
        .withDocumentationUrl(URI(DOCUMENTATION_URL))
        .withConnectionSpecification(deserialize(Files.readString(oAuthSpecificationPath)))
        .withAdvancedAuth(advancedAuth)
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
    } catch (e: URISyntaxException) {
      throw RuntimeException(e)
    }
  }
}
