/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.stubs;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@SuppressWarnings("MissingJavadocType")
@ActivityInterface
public interface TestActivity {

  @ActivityMethod
  String getValue();

}
