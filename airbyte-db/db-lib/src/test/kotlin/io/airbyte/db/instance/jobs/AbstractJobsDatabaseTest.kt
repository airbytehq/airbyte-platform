/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs

import io.airbyte.db.Database
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.test.utils.AbstractDatabaseTest
import org.jooq.DSLContext
import javax.sql.DataSource

abstract class AbstractJobsDatabaseTest : AbstractDatabaseTest() {
  override fun createDatabase(
    dataSource: DataSource,
    dslContext: DSLContext,
  ): Database = TestDatabaseProviders(dataSource, dslContext).turnOffMigration().createNewJobsDatabase()
}
