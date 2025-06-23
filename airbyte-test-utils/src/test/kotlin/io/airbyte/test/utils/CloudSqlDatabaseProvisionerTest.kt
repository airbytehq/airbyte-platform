/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.sqladmin.SQLAdmin
import com.google.api.services.sqladmin.model.Database
import com.google.api.services.sqladmin.model.Operation
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import java.io.IOException
import java.util.concurrent.Callable

@ExtendWith(MockitoExtension::class)
internal class CloudSqlDatabaseProvisionerTest {
  @Mock
  private val sqlAdmin: SQLAdmin? = null

  @Mock
  private val databases: SQLAdmin.Databases? = null

  @Mock
  private val operations: SQLAdmin.Operations? = null

  @Mock
  private val getOperation: SQLAdmin.Operations.Get? = null

  @Mock
  private val insertDatabase: SQLAdmin.Databases.Insert? = null

  @Mock
  private val deleteDatabase: SQLAdmin.Databases.Delete? = null

  @Mock
  private val operation: Operation? = null

  @Mock
  private val googleJsonResponseException: GoogleJsonResponseException? = null

  @Mock
  private val callable: Callable<Operation>? = null

  lateinit var provisioner: CloudSqlDatabaseProvisioner

  @BeforeEach
  fun setUp() {
    provisioner = CloudSqlDatabaseProvisioner(sqlAdmin!!, POLL_ATTEMPTS, API_CALL_ATTEMPTS)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testCreateDatabase() {
    mockOperation()
    Mockito.`when`(operation!!.status).thenReturn("DONE")
    Mockito.`when`(sqlAdmin!!.databases()).thenReturn(databases)
    Mockito
      .`when`(
        databases!!.insert(
          ArgumentMatchers.anyString(),
          ArgumentMatchers.anyString(),
          ArgumentMatchers.any(
            Database::class.java,
          ),
        ),
      ).thenReturn(insertDatabase)
    Mockito.`when`(insertDatabase!!.execute()).thenReturn(operation)
    Mockito.`when`(operation.name).thenReturn("operation-name")

    provisioner.createDatabase(PROJECT_ID, INSTANCE_ID, DATABASE_NAME)

    Mockito.verify(databases).insert(PROJECT_ID, INSTANCE_ID, Database().setName(DATABASE_NAME))
    Mockito.verify(insertDatabase).execute()
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testDeleteDatabase() {
    mockOperation()
    Mockito.`when`(operation!!.status).thenReturn("DONE")
    Mockito.`when`(sqlAdmin!!.databases()).thenReturn(databases)
    Mockito
      .`when`(databases!!.delete(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
      .thenReturn(deleteDatabase)
    Mockito.`when`(deleteDatabase!!.execute()).thenReturn(operation)
    Mockito.`when`(operation.name).thenReturn("operation-name")

    provisioner.deleteDatabase(PROJECT_ID, INSTANCE_ID, DATABASE_NAME)

    Mockito.verify(databases).delete(PROJECT_ID, INSTANCE_ID, DATABASE_NAME)
    Mockito.verify(deleteDatabase).execute()
  }

  @Test
  @Throws(IOException::class)
  fun testPollOperationNotDoneAfterMaxStatusChecks() {
    mockOperation()
    Mockito
      .`when`(operation!!.status)
      .thenReturn("PENDING")
      .thenReturn("RUNNING")
      .thenReturn("DONE")
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { provisioner.pollOperation(PROJECT_ID, "operation-name") }
  }

  @Test
  @Throws(IOException::class)
  fun testPollOperationDoneBeforeMaxStatusChecks() {
    mockOperation()
    Mockito
      .`when`(operation!!.status)
      .thenReturn("PENDING")
      .thenReturn("DONE")
    Assertions.assertDoesNotThrow {
      provisioner.pollOperation(
        PROJECT_ID,
        "operation-name",
      )
    }
  }

  @Throws(IOException::class)
  private fun mockOperation() {
    Mockito.`when`(sqlAdmin!!.operations()).thenReturn(operations)
    Mockito
      .`when`(
        operations!![
          ArgumentMatchers.eq(
            PROJECT_ID,
          ), ArgumentMatchers.anyString(),
        ],
      ).thenReturn(getOperation)
    Mockito.`when`(getOperation!!.execute()).thenReturn(operation)
  }

  @Test
  @Throws(Exception::class)
  fun testMoreThanMaxAttempts() {
    Mockito.`when`(callable!!.call()).thenThrow(googleJsonResponseException)
    Mockito.`when`(googleJsonResponseException!!.statusCode).thenReturn(409)
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { provisioner.runWithRetry(callable) }
  }

  @Test
  @Throws(Exception::class)
  fun testNoRetry() {
    Mockito.`when`(callable!!.call()).thenThrow(RuntimeException())
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { provisioner.runWithRetry(callable) }
  }

  @Test
  @Throws(Exception::class)
  fun testOneRetry() {
    Mockito.`when`(googleJsonResponseException!!.statusCode).thenReturn(409)
    Mockito
      .`when`(callable!!.call())
      .thenThrow(googleJsonResponseException)
      .thenReturn(Operation())

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
