/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license.annotation

import io.airbyte.commons.license.condition.AirbyteProEnabledCondition
import io.micronaut.context.annotation.Requires
import java.lang.annotation.Inherited

/**
 * Annotation that indicates that a bean requires Airbyte Pro to be enabled.
 */
@MustBeDocumented
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.PROPERTY_SETTER)
@Inherited
@Requires(condition = AirbyteProEnabledCondition::class)
annotation class RequiresAirbyteProEnabled
