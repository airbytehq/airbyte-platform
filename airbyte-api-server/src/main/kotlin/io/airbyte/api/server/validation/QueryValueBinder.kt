/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.validation

import io.micronaut.core.bind.ArgumentBinder.BindingResult
import io.micronaut.core.convert.ArgumentConversionContext
import io.micronaut.core.convert.ConversionError
import io.micronaut.core.convert.ConversionService
import io.micronaut.http.bind.binders.QueryValueArgumentBinder
import java.util.Optional

/**
 * Required to force validation of nullable query string parameters. Taken from
 * https://github.com/micronaut-projects/micronaut-core/issues/5135. Should be replaced when
 * micronaut validation is improved. https://github.com/micronaut-projects/micronaut-core/pull/6808
 */
class QueryValueBinder<T>(conversionService: ConversionService<*>?) : QueryValueArgumentBinder<T>(conversionService) {
  override fun doConvert(
    value: Any?,
    context: ArgumentConversionContext<T>,
    defaultResult: BindingResult<T>,
  ): BindingResult<T> {
    return if (value == null && context.hasErrors()) {
      object : BindingResult<T> {
        override fun getValue(): Optional<T>? {
          return null
        }

        override fun isSatisfied(): Boolean {
          return false
        }

        override fun getConversionErrors(): List<ConversionError> {
          val errors: MutableList<ConversionError> = ArrayList()
          for (error in context) {
            errors.add(error)
          }
          return errors
        }
      }
    } else {
      super.doConvert(value, context, defaultResult)
    }
  }
}
