/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license.annotation

import io.airbyte.commons.license.condition.AirbyteProEnabledCondition
import io.airbyte.commons.license.condition.VerifiedProLicenseCondition
import io.micronaut.context.annotation.Requires
import java.lang.annotation.Inherited

/**
 * Annotation that indicates that a bean requires a verified Airbyte Pro license to be enabled.
 */
@MustBeDocumented
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
@Inherited
@Requires(condition = AirbyteProEnabledCondition::class)
@Requires(condition = VerifiedProLicenseCondition::class)
annotation class RequiresVerifiedAirbyteProLicense
