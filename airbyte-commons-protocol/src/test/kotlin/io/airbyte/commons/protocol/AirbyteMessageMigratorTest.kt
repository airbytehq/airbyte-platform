/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration
import io.airbyte.commons.version.Version
import io.airbyte.config.ConfiguredAirbyteCatalog
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional

internal class AirbyteMessageMigratorTest {
  @JvmRecord
  internal data class ObjectV0(
    val name0: String,
  )

  @JvmRecord
  internal data class ObjectV1(
    val name1: String,
  )

  @JvmRecord
  internal data class ObjectV2(
    val name2: String,
  )

  internal class Migrate0to1 : AirbyteMessageMigration<ObjectV0, ObjectV1> {
    override fun downgrade(
      message: ObjectV1,
      configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
    ): ObjectV0 = ObjectV0(message.name1)

    override fun upgrade(
      message: ObjectV0,
      configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
    ): ObjectV1 = ObjectV1(message.name0)

    override fun getPreviousVersion(): Version = v0

    override fun getCurrentVersion(): Version = v1
  }

  internal class Migrate1to2 : AirbyteMessageMigration<ObjectV1, ObjectV2> {
    override fun downgrade(
      message: ObjectV2,
      configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
    ): ObjectV1 = ObjectV1(message.name2)

    override fun upgrade(
      message: ObjectV1,
      configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog>,
    ): ObjectV2 = ObjectV2(message.name1)

    override fun getPreviousVersion(): Version = v1

    override fun getCurrentVersion(): Version = v2
  }

  private lateinit var migrator: AirbyteMessageMigrator

  @BeforeEach
  fun beforeEach() {
    migrator = AirbyteMessageMigrator(listOf(Migrate0to1(), Migrate1to2()))
    migrator.initialize()
  }

  @Test
  fun testDowngrade() {
    val obj = ObjectV2("my name")

    val objDowngradedTo0 = migrator.downgrade<ObjectV0, ObjectV2>(obj, v0, Optional.empty())
    Assertions.assertEquals(obj.name2, objDowngradedTo0.name0)

    val objDowngradedTo1 = migrator.downgrade<ObjectV1, ObjectV2>(obj, v1, Optional.empty())
    Assertions.assertEquals(obj.name2, objDowngradedTo1.name1)

    val objDowngradedTo2 = migrator.downgrade<ObjectV2, ObjectV2>(obj, v2, Optional.empty())
    Assertions.assertEquals(obj.name2, objDowngradedTo2.name2)
  }

  @Test
  fun testUpgrade() {
    val obj0 = ObjectV0("my name 0")
    val objUpgradedFrom0 = migrator.upgrade<ObjectV0, ObjectV2>(obj0, v0, Optional.empty())
    Assertions.assertEquals(obj0.name0, objUpgradedFrom0.name2)

    val obj1 = ObjectV1("my name 1")
    val objUpgradedFrom1 = migrator.upgrade<ObjectV1, ObjectV2>(obj1, v1, Optional.empty())
    Assertions.assertEquals(obj1.name1, objUpgradedFrom1.name2)

    val obj2 = ObjectV2("my name 2")
    val objUpgradedFrom2 = migrator.upgrade<ObjectV2, ObjectV2>(obj2, v2, Optional.empty())
    Assertions.assertEquals(obj2.name2, objUpgradedFrom2.name2)
  }

  @Test
  fun testUnsupportedDowngradeShouldFailExplicitly() {
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) {
      migrator.downgrade<Any, ObjectV2>(
        ObjectV2("woot"),
        Version("5.0.0"),
        Optional.empty(),
      )
    }
  }

  @Test
  fun testUnsupportedUpgradeShouldFailExplicitly() {
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) {
      migrator.upgrade<ObjectV0, Any>(
        ObjectV0("woot"),
        Version("4.0.0"),
        Optional.empty(),
      )
    }
  }

  @Test
  fun testRegisterCollisionsShouldFail() {
    Assertions.assertThrows(RuntimeException::class.java) {
      migrator = AirbyteMessageMigrator(listOf(Migrate0to1(), Migrate1to2(), Migrate0to1()))
      migrator.initialize()
    }
  }

  companion object {
    val v0: Version = Version("0.0.0")
    val v1: Version = Version("1.0.0")
    val v2: Version = Version("2.0.0")
  }
}
