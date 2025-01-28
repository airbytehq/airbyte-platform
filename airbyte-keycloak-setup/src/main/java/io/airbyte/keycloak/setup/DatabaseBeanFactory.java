/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.data.services.shared.DataSourceUnwrapper;
import io.airbyte.db.Database;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.jooq.DSLContext;

@Factory
public class DatabaseBeanFactory {

  @Singleton
  @Named("configDatabase")
  public Database configDatabase(@Named("config") final DSLContext dslContext) {
    return new Database(DataSourceUnwrapper.unwrapContext(dslContext));
  }

}
