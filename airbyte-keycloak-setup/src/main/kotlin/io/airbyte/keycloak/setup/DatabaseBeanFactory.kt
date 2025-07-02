/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.data.services.shared.DataSourceUnwrapper
import io.airbyte.db.Database
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext

@Factory
class DatabaseBeanFactory {
  @Singleton
  @Named("configDatabase")
  fun configDatabase(
    @Named("config") dslContext: DSLContext,
  ): Database = Database(DataSourceUnwrapper.unwrapContext(dslContext))
}
