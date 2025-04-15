/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import com.zaxxer.hikari.HikariDataSource
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.factory.DataSourceFactory.create
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import javax.sql.DataSource
import kotlin.test.assertIs

private const val CONNECT_TIMEOUT = "connectTimeout"

/**
 * Test suite for the [DataSourceFactory] class.
 */
internal class DataSourceFactoryTest : CommonFactoryTest() {
  @Test
  fun testCreatingDataSourceWithConnectionTimeoutSetAboveDefault() {
    val connectionProperties = mapOf(CONNECT_TIMEOUT to "61")
    val dataSource =
      create(
        username = username,
        password = password,
        driverClassName = driverClassName,
        jdbcConnectionString = jdbcUrl,
        connectionProperties = connectionProperties,
      )
    assertNotNull(dataSource)
    assertIs<HikariDataSource>(dataSource)
    assertEquals(61000, dataSource.hikariConfigMXBean.connectionTimeout)
  }

  @Test
  fun testCreatingPostgresDataSourceWithConnectionTimeoutSetBelowDefault() {
    val connectionProperties = mapOf(CONNECT_TIMEOUT to "30")
    val dataSource =
      create(
        username = username,
        password = password,
        driverClassName = driverClassName,
        jdbcConnectionString = jdbcUrl,
        connectionProperties = connectionProperties,
      )
    assertNotNull(dataSource)
    assertIs<HikariDataSource>(dataSource)
    assertEquals(30000, dataSource.hikariConfigMXBean.connectionTimeout)
  }

  @Test
  fun testCreatingDataSourceWithConnectionTimeoutSetWithZero() {
    val connectionProperties = mapOf(CONNECT_TIMEOUT to "0")
    val dataSource =
      create(
        username = username,
        password = password,
        driverClassName = driverClassName,
        jdbcConnectionString = jdbcUrl,
        connectionProperties = connectionProperties,
      )
    assertNotNull(dataSource)
    assertIs<HikariDataSource>(dataSource)
    assertEquals(Int.MAX_VALUE.toLong(), dataSource.hikariConfigMXBean.connectionTimeout)
  }

  @Test
  fun testCreatingPostgresDataSourceWithConnectionTimeoutNotSet() {
    val connectionProperties = mapOf<String, String>()
    val dataSource =
      create(
        username = username,
        password = password,
        driverClassName = driverClassName,
        jdbcConnectionString = jdbcUrl,
        connectionProperties = connectionProperties,
      )
    assertNotNull(dataSource)
    assertIs<HikariDataSource>(dataSource)
    assertEquals(10000, dataSource.hikariConfigMXBean.connectionTimeout)
  }

  @Test
  fun testCreatingADataSourceWithJdbcUrl() {
    val dataSource =
      create(
        username = username,
        password = password,
        driverClassName = driverClassName,
        jdbcConnectionString = jdbcUrl,
      )
    assertNotNull(dataSource)
    assertIs<HikariDataSource>(dataSource)
    assertEquals(10, dataSource.hikariConfigMXBean.maximumPoolSize)
  }

  @Test
  fun testCreatingADataSourceWithJdbcUrlAndConnectionProperties() {
    val connectionProperties = mapOf("foo" to "bar")

    val dataSource =
      create(
        username = username,
        password = password,
        driverClassName = driverClassName,
        jdbcConnectionString = jdbcUrl,
        connectionProperties = connectionProperties,
      )
    assertNotNull(dataSource)
    assertIs<HikariDataSource>(dataSource)
    assertEquals(10, dataSource.hikariConfigMXBean.maximumPoolSize)
  }

  @Test
  fun testClosingADataSource() {
    val dataSource1 =
      mockk<HikariDataSource> {
        every { close() } returns Unit
      }
    assertDoesNotThrow { close(dataSource1) }
    verify(exactly = 1) { dataSource1.close() }

    val dataSource2 = mockk<DataSource>()
    assertDoesNotThrow { close(dataSource2) }

    assertDoesNotThrow { close(null) }
  }

  companion object {
    lateinit var database: String
    lateinit var driverClassName: String
    lateinit var host: String
    lateinit var jdbcUrl: String
    lateinit var password: String
    lateinit var username: String
    var port: Int = 0

    @BeforeAll
    @JvmStatic
    fun setup() {
      host = container.host
      port = container.firstMappedPort
      database = container.databaseName
      username = container.username
      password = container.password
      driverClassName = container.driverClassName
      jdbcUrl = container.jdbcUrl
    }
  }
}
