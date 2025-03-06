/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.sqladmin.SQLAdmin;
import com.google.api.services.sqladmin.SQLAdmin.Operations;
import com.google.api.services.sqladmin.model.Database;
import com.google.api.services.sqladmin.model.Operation;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"PMD.LooseCoupling", "PMD.AvoidDuplicateLiterals"})
class CloudSqlDatabaseProvisionerTest {

  private static final String PROJECT_ID = "project-id";
  private static final String INSTANCE_ID = "instance-id";
  private static final String DATABASE_NAME = "database-name";
  private static final int POLL_ATTEMPTS = 2;
  private static final int API_CALL_ATTEMPTS = 2;

  @Mock
  private SQLAdmin sqlAdmin;
  @Mock
  private SQLAdmin.Databases databases;
  @Mock
  private Operations operations;
  @Mock
  private Operations.Get getOperation;
  @Mock
  private SQLAdmin.Databases.Insert insertDatabase;
  @Mock
  private SQLAdmin.Databases.Delete deleteDatabase;
  @Mock
  private Operation operation;
  @Mock
  private GoogleJsonResponseException googleJsonResponseException;
  @Mock
  private Callable callable;

  private CloudSqlDatabaseProvisioner provisioner;

  @BeforeEach
  void setUp() {
    provisioner = new CloudSqlDatabaseProvisioner(sqlAdmin, POLL_ATTEMPTS, API_CALL_ATTEMPTS);
  }

  @Test
  void testCreateDatabase() throws IOException, InterruptedException {
    mockOperation();
    when(operation.getStatus()).thenReturn("DONE");
    when(sqlAdmin.databases()).thenReturn(databases);
    when(databases.insert(anyString(), anyString(), any(Database.class))).thenReturn(insertDatabase);
    when(insertDatabase.execute()).thenReturn(operation);
    when(operation.getName()).thenReturn("operation-name");

    provisioner.createDatabase(PROJECT_ID, INSTANCE_ID, DATABASE_NAME);

    verify(databases).insert(PROJECT_ID, INSTANCE_ID, new Database().setName(DATABASE_NAME));
    verify(insertDatabase).execute();
  }

  @Test
  void testDeleteDatabase() throws IOException, InterruptedException {
    mockOperation();
    when(operation.getStatus()).thenReturn("DONE");
    when(sqlAdmin.databases()).thenReturn(databases);
    when(databases.delete(anyString(), anyString(), anyString())).thenReturn(deleteDatabase);
    when(deleteDatabase.execute()).thenReturn(operation);
    when(operation.getName()).thenReturn("operation-name");

    provisioner.deleteDatabase(PROJECT_ID, INSTANCE_ID, DATABASE_NAME);

    verify(databases).delete(PROJECT_ID, INSTANCE_ID, DATABASE_NAME);
    verify(deleteDatabase).execute();
  }

  @Test
  void testPollOperationNotDoneAfterMaxStatusChecks() throws IOException {
    mockOperation();
    when(operation.getStatus())
        .thenReturn("PENDING")
        .thenReturn("RUNNING")
        .thenReturn("DONE");
    assertThrows(RuntimeException.class, () -> provisioner.pollOperation(PROJECT_ID, "operation-name"));
  }

  @Test
  void testPollOperationDoneBeforeMaxStatusChecks() throws IOException {
    mockOperation();
    when(operation.getStatus())
        .thenReturn("PENDING")
        .thenReturn("DONE");
    assertDoesNotThrow(() -> provisioner.pollOperation(PROJECT_ID, "operation-name"));
  }

  private void mockOperation() throws IOException {
    when(sqlAdmin.operations()).thenReturn(operations);
    when(operations.get(eq(PROJECT_ID), anyString())).thenReturn(getOperation);
    when(getOperation.execute()).thenReturn(operation);
  }

  @Test
  void testMoreThanMaxAttempts() throws Exception {
    when(callable.call()).thenThrow(googleJsonResponseException);
    when(googleJsonResponseException.getStatusCode()).thenReturn(409);
    assertThrows(RuntimeException.class, () -> provisioner.runWithRetry(callable));
  }

  @Test
  void testNoRetry() throws Exception {
    when(callable.call()).thenThrow(new RuntimeException());
    assertThrows(RuntimeException.class, () -> provisioner.runWithRetry(callable));
  }

  @Test
  void testOneRetry() throws Exception {
    when(googleJsonResponseException.getStatusCode()).thenReturn(409);
    when(callable.call())
        .thenThrow(googleJsonResponseException)
        .thenReturn(null);

    assertDoesNotThrow(() -> provisioner.runWithRetry(callable));
  }

}
