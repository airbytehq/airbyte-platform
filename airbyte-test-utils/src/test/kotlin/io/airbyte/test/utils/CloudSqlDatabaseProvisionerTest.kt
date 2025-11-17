/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.sqladmin.SQLAdmin
import com.google.api.services.sqladmin.model.Database
import com.google.api.services.sqladmin.model.Operation
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.Callable

@ExtendWith(MockKExtension::class)
internal class CloudSqlDatabaseProvisionerTest {
  @MockK
  private lateinit var sqlAdmin: SQLAdmin

  @MockK
  private lateinit var databases: SQLAdmin.Databases

  @MockK
  private lateinit var operations: SQLAdmin.Operations

  @MockK
  private lateinit var getOperation: SQLAdmin.Operations.Get

  @MockK
  private lateinit var insertDatabase: SQLAdmin.Databases.Insert

  @MockK
  private lateinit var deleteDatabase: SQLAdmin.Databases.Delete

  @MockK
  private lateinit var operation: Operation

  @MockK
  private lateinit var googleJsonResponseException: GoogleJsonResponseException

  @MockK
  private lateinit var callable: Callable<Operation>

  lateinit var provisioner: CloudSqlDatabaseProvisioner

  @BeforeEach
  fun setUp() {
    provisioner = CloudSqlDatabaseProvisioner(sqlAdmin, POLL_ATTEMPTS, API_CALL_ATTEMPTS)
  }

  @Test
  fun testCreateDatabase() {
    mockOperation()
    every { operation.status } returns "DONE"
    every { sqlAdmin.databases() } returns databases
    every { databases.insert(any<String>(), any<String>(), any<Database>()) } returns insertDatabase
    every { insertDatabase.execute() } returns operation
    every { operation.name } returns "operation-name"

    provisioner.createDatabase(PROJECT_ID, INSTANCE_ID, DATABASE_NAME)

    verify { databases.insert(PROJECT_ID, INSTANCE_ID, Database().setName(DATABASE_NAME)) }
    verify { insertDatabase.execute() }
  }

  @Test
  fun testDeleteDatabase() {
    mockOperation()
    every { operation.status } returns "DONE"
    every { sqlAdmin.databases() } returns databases
    every { databases.delete(any<String>(), any<String>(), any<String>()) } returns deleteDatabase
    every { deleteDatabase.execute() } returns operation
    every { operation.name } returns "operation-name"

    provisioner.deleteDatabase(PROJECT_ID, INSTANCE_ID, DATABASE_NAME)

    verify { databases.delete(PROJECT_ID, INSTANCE_ID, DATABASE_NAME) }
    verify { deleteDatabase.execute() }
  }

  @Test
  fun testPollOperationNotDoneAfterMaxStatusChecks() {
    mockOperation()
    every { operation.status } returnsMany listOf("PENDING", "RUNNING", "DONE")
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { provisioner.pollOperation(PROJECT_ID, "operation-name") }
  }

  @Test
  fun testPollOperationDoneBeforeMaxStatusChecks() {
    mockOperation()
    every { operation.status } returnsMany listOf("PENDING", "DONE")
    Assertions.assertDoesNotThrow {
      provisioner.pollOperation(
        PROJECT_ID,
        "operation-name",
      )
    }
  }

  private fun mockOperation() {
    every { sqlAdmin.operations() } returns operations
    every { operations.get(PROJECT_ID, any<String>()) } returns getOperation
    every { getOperation.execute() } returns operation
  }

  @Test
  fun testMoreThanMaxAttempts() {
    every { callable.call() } throws googleJsonResponseException
    every { googleJsonResponseException.statusCode } returns 409
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { provisioner.runWithRetry(callable) }
  }

  @Test
  fun testNoRetry() {
    every { callable.call() } throws RuntimeException()
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { provisioner.runWithRetry(callable) }
  }

  @Test
  fun testOneRetry() {
    every { googleJsonResponseException.statusCode } returns 409
    every { callable.call() } throws googleJsonResponseException andThen Operation()

    Assertions.assertDoesNotThrow<Operation> { provisioner.runWithRetry(callable) }
  }

  companion object {
    private const val PROJECT_ID = "project-id"
    private const val INSTANCE_ID = "instance-id"
    private const val DATABASE_NAME = "database-name"
    private const val POLL_ATTEMPTS = 2
    private const val API_CALL_ATTEMPTS = 2
  }
}
