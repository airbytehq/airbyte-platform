/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import io.airbyte.api.common.StreamDescriptorUtils.buildFieldName
import io.airbyte.api.common.StreamDescriptorUtils.buildFullyQualifiedName
import io.airbyte.api.model.generated.StreamDescriptor
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class StreamDescriptorUtilsTest {
  @Test
  fun testBuildFullyQualifiedNameWithNamespace() {
    val descriptor = StreamDescriptor().name("main_name").namespace("namespace")
    val fqn = buildFullyQualifiedName(descriptor)
    Assertions.assertEquals("namespace.main_name", fqn)
  }

  @Test
  fun testBuildFullyQualifiedNameNoNamespace() {
    val descriptor = StreamDescriptor().name("foobar")
    val fqn = buildFullyQualifiedName(descriptor)
    Assertions.assertEquals("foobar", fqn)
  }

  @Test
  fun testBuildFieldName() {
    Assertions.assertEquals("root.branch.value", buildFieldName(mutableListOf<String>("root", "branch", "value")))
    Assertions.assertEquals("single_value", buildFieldName(mutableListOf<String>("single_value")))
  }
}
