/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client;

import io.airbyte.api.common.StreamDescriptorUtils;
import io.airbyte.api.model.generated.StreamDescriptor;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StreamDescriptorUtilsTest {

  @Test
  void testBuildFullyQualifiedNameWithNamespace() {
    StreamDescriptor descriptor = new StreamDescriptor().name("main_name").namespace("namespace");
    String fqn = StreamDescriptorUtils.buildFullyQualifiedName(descriptor);
    Assertions.assertEquals("namespace.main_name", fqn);
  }

  @Test
  void testBuildFullyQualifiedNameNoNamespace() {
    StreamDescriptor descriptor = new StreamDescriptor().name("foobar");
    String fqn = StreamDescriptorUtils.buildFullyQualifiedName(descriptor);
    Assertions.assertEquals("foobar", fqn);
  }

  @Test
  void testBuildFieldName() {
    Assertions.assertEquals("root.branch.value", StreamDescriptorUtils.buildFieldName(List.of("root", "branch", "value")));
    Assertions.assertEquals("single_value", StreamDescriptorUtils.buildFieldName(List.of("single_value")));
  }

}
