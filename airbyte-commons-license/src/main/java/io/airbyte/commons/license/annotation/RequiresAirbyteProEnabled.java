/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license.annotation;

import io.airbyte.commons.license.condition.AirbyteProEnabledCondition;
import io.micronaut.context.annotation.Requires;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that indicates that a bean requires Airbyte Pro to be enabled.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
@Requires(condition = AirbyteProEnabledCondition.class)
public @interface RequiresAirbyteProEnabled {

}
