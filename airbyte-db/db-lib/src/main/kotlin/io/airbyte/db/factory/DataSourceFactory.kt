/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.airbyte.db.factory.DatabaseDriver.Companion.findByDriverClassName
import org.postgresql.PGProperty
import javax.sql.DataSource
import kotlin.time.Duration.Companion.seconds

/**
 * Temporary factory class that provides convenience methods for creating a [DataSource]
 * instance. This class will be removed once the project has been converted to leverage an
 * application framework to manage the creation and injection of [DataSource] objects.
 */
object DataSourceFactory {
  /**
   * Constructs a new [DataSource] from a [DataSourceConfig].
   */
  @JvmStatic
  fun create(cfg: DataSourceConfig): DataSource = cfg.dataSource()

  /**
   * Constructs a new [DataSource] using the provided configuration.
   *
   * @param username The username of the database user.
   * @param password The password of the database user.
   * @param driverClassName The fully qualified name of the JDBC driver class.
   * @param jdbcConnectionString The JDBC connection string.
   * @param connectionProperties Additional configuration properties for the underlying driver.
   * @return The configured [DataSource].
   */
  @JvmStatic
  @JvmOverloads
  fun create(
    username: String,
    password: String,
    driverClassName: String,
    jdbcConnectionString: String,
    connectionProperties: Map<String, String> = mapOf(),
  ): DataSource =
    DataSourceConfig(
      driverClassName = driverClassName,
      jdbcUrl = jdbcConnectionString,
      username = username,
      password = password,
      connectionProperties = connectionProperties,
    ).dataSource()

  /**
   * Utility method that attempts to close the provided [DataSource] if it implements [Closeable].
   *
   * @param dataSource The [DataSource] to close.
   * @throws Exception if unable to close the data source.
   */
  @JvmStatic
  fun close(dataSource: DataSource?) {
    dataSource?.let {
      if (it is AutoCloseable) {
        it.close()
      }
    }
  }
}

private const val CONNECT_TIMEOUT_KEY = "connectTimeout"
private val CONNECT_TIMEOUT_DEFAULT = 60.seconds.inWholeMilliseconds

data class DataSourceConfig(
  val driverClassName: String,
  val username: String,
  val password: String,
  val port: Int = 5432,
  val maximumPoolSize: Int = 10,
  val minimumPoolSize: Int = 0,
  val connectionProperties: Map<String, String> = mapOf(),
  val database: String? = null,
  val host: String? = null,
  val jdbcUrl: String? = null,
) {
  val databaseDriver: DatabaseDriver =
    DatabaseDriver.findByDriverClassName(driverClassName)
      ?: throw IllegalArgumentException("unknown or blank driver class name: '$driverClassName'")

  // Set the connectionTimeout value from connection properties in seconds, default minimum timeout of [CONNECT_TIMEOUT_DEFAULT]
  // since Hikari default of 30 seconds is not enough for acceptance tests.
  //
  // In the case the value is 0, pass the value along as Hikari and Postgres use default max value for 0 timeout value.
  //
  // NOTE: HikariCP uses milliseconds for all time values: https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
  // whereas Postgres is measured in seconds: https://jdbc.postgresql.org/documentation/head/connect.html
  val connectionTimeoutMs: Long =
    when {
      databaseDriver == DatabaseDriver.POSTGRESQL -> {
        // NOTE: do not change this `.getName()` call to `.name`.  `.name` will return then name of the enum not the name property on the enum.
        val pgPropertyConnectTimeout = PGProperty.CONNECT_TIMEOUT.getName()
        connectionProperties[pgPropertyConnectTimeout]
          ?.toLong()
          ?.takeIf { it >= 0 }
          ?.seconds
          ?.inWholeMilliseconds
          ?: PGProperty.CONNECT_TIMEOUT.defaultValue
            ?.toLong()
            ?.seconds
            ?.inWholeMilliseconds
          ?: throw IllegalArgumentException("unable to determine postgresql connection timeout")
      }
      else -> {
        val connectTimeout = connectionProperties[CONNECT_TIMEOUT_KEY]?.toLong()?.seconds?.inWholeMilliseconds ?: CONNECT_TIMEOUT_DEFAULT

        when {
          connectTimeout == 0L -> 0L
          connectTimeout > CONNECT_TIMEOUT_DEFAULT -> connectTimeout
          else -> CONNECT_TIMEOUT_DEFAULT
        }
      }
    }

  init {
    // one of the two must be true
    // 1. jdbcUrl is defined
    // 2. host && database are defined
    if (jdbcUrl == null && (host == null || database == null)) {
      throw IllegalArgumentException("either the jdbcUrl alone or the host and database must be specified")
    }
  }
}

/**
 * Declared as an extension method so that it can be declared private but still be accessed by the [DataSourceFactory].
 */
private fun DataSourceConfig.dataSource(): DataSource {
  val config =
    HikariConfig().apply {
      driverClassName = databaseDriver.driverClassName
      // the init method on the [DataSourceConfig] should prevent the !! from throwing here
      jdbcUrl = this@dataSource.jdbcUrl ?: databaseDriver.url(host!!, port, database!!)
      maximumPoolSize = this@dataSource.maximumPoolSize
      minimumIdle = this@dataSource.minimumPoolSize
      connectionTimeout = this@dataSource.connectionTimeoutMs
      password = this@dataSource.password
      username = this@dataSource.username
      // Expose stats via JMX (https://github.com/brettwooldridge/HikariCP/wiki/MBean-(JMX)-Monitoring-and-Management)
      isRegisterMbeans = true

      /*
       * Disable to prevent failing on startup. Applications may start prior to the database container
       * being available. To avoid failing to create the connection pool, disable the fail check. This
       * will preserve existing behavior that tests for the connection on first use, not on creation.
       */
      initializationFailTimeout = Int.MIN_VALUE.toLong()

      this@dataSource.connectionProperties.forEach { (propertyName, value) -> addDataSourceProperty(propertyName, value) }
    }

  return HikariDataSource(config)
}
