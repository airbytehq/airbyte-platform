/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.validation

import io.micronaut.context.annotation.Replaces
import io.micronaut.core.convert.ConversionService
import io.micronaut.http.bind.DefaultRequestBinderRegistry
import io.micronaut.http.bind.binders.RequestArgumentBinder
import jakarta.inject.Singleton

/**
 * Required to force validation of nullable query string parameters. Taken from
 * https://github.com/micronaut-projects/micronaut-core/issues/5135. Should be replaced when
 * micronaut validation is improved. https://github.com/micronaut-projects/micronaut-core/pull/6808
 */
@Singleton
@Replaces(DefaultRequestBinderRegistry::class)
class AirbyteRequestBinderRegistry(conversionService: ConversionService<*>?, binders: List<RequestArgumentBinder<*>?>?) :
  DefaultRequestBinderRegistry(conversionService, binders) {
  init {
    addRequestArgumentBinder(QueryValueBinder<Any>(conversionService))
  }
}
