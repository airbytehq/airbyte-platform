/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import static io.airbyte.commons.constants.AirbyteCatalogConstants.LOCAL_SECRETS_MASKS_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.yaml.Yamls;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link ConnectorSpecMaskGenerator} class.
 */
class ConnectorSpecMaskGeneratorTest {

  private String getProjectAbsolutePath() {
    return new File("").getAbsolutePath();
  }

  @Test
  void testConnectorSpecMaskGenerator() throws IOException {
    // final String directory = "src/test/resources/";
    final String directory = "src/test/resources/valid_specs";
    final File outputFile = new File(directory, LOCAL_SECRETS_MASKS_PATH);

    final String[] args = {"--resource-root", directory};
    // log output file path
    System.out.println("put file here");
    System.out.println(outputFile.getAbsolutePath());
    ConnectorSpecMaskGenerator.main(args);
    assertTrue(outputFile.exists());

    final JsonNode maskContents = Yamls.deserialize(FileUtils.readFileToString(outputFile, Charset.defaultCharset()));
    assertEquals(Set.of("azure_blob_storage_account_key", "api_key"),
        Jsons.object(maskContents.get("properties"), new TypeReference<Set<String>>() {}));
  }

  @Test
  void testConnectorSpecMaskGeneratorNoSpecs() {
    final String directory = "src/test/resources/no_specs";
    final File outputFile = new File(directory, LOCAL_SECRETS_MASKS_PATH);
    final String[] args = {"--project-root", directory};
    ConnectorSpecMaskGenerator.main(args);
    assertFalse(outputFile.exists());
  }

}
