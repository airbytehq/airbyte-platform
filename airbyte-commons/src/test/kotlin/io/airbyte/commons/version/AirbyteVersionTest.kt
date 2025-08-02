/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

internal class AirbyteVersionTest {
  @Test
  fun testParseVersion() {
    val version = AirbyteVersion(VERSION_678)
    Assertions.assertEquals("6", version.getMajorVersion())
    Assertions.assertEquals("7", version.getMinorVersion())
    Assertions.assertEquals("8", version.getPatchVersion())
  }

  @Test
  fun testParseVersionWithLabel() {
    val version = AirbyteVersion(VERSION_678_OMEGA)
    Assertions.assertEquals("6", version.getMajorVersion())
    Assertions.assertEquals("7", version.getMinorVersion())
    Assertions.assertEquals("8", version.getPatchVersion())
  }

  @Test
  fun testCompatibleVersionCompareTo() {
    Assertions.assertEquals(0, AirbyteVersion(VERSION_678_OMEGA).compatibleVersionCompareTo(AirbyteVersion(VERSION_678_GAMMA)))
    Assertions.assertEquals(0, AirbyteVersion(VERSION_678_ALPHA).compatibleVersionCompareTo(AirbyteVersion(VERSION_679_ALPHA)))
    Assertions.assertTrue(0 < AirbyteVersion(VERSION_680_ALPHA).compatibleVersionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(0 < AirbyteVersion("11.8.0-alpha").compatibleVersionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(0 < AirbyteVersion(VERSION_6110_ALPHA).compatibleVersionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(0 > AirbyteVersion("0.8.0-alpha").compatibleVersionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertEquals(0, AirbyteVersion(VERSION_123_PROD).compatibleVersionCompareTo(AirbyteVersion(DEV)))
    Assertions.assertEquals(0, AirbyteVersion(DEV).compatibleVersionCompareTo(AirbyteVersion(VERSION_123_PROD)))
  }

  @Test
  fun testversionCompareTo() {
    Assertions.assertEquals(0, AirbyteVersion(VERSION_678_OMEGA).versionCompareTo(AirbyteVersion(VERSION_678_GAMMA)))
    Assertions.assertTrue(0 > AirbyteVersion(VERSION_678_ALPHA).versionCompareTo(AirbyteVersion(VERSION_679_ALPHA)))
    Assertions.assertTrue(0 > AirbyteVersion(VERSION_678_ALPHA).versionCompareTo(AirbyteVersion("6.7.11-alpha")))
    Assertions.assertTrue(0 < AirbyteVersion(VERSION_680_ALPHA).versionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(0 < AirbyteVersion(VERSION_6110_ALPHA).versionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(0 > AirbyteVersion(VERSION_380_ALPHA).versionCompareTo(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(0 > AirbyteVersion(VERSION_380_ALPHA).versionCompareTo(AirbyteVersion("11.7.8-alpha")))
    Assertions.assertEquals(0, AirbyteVersion(VERSION_123_PROD).versionCompareTo(AirbyteVersion(DEV)))
    Assertions.assertEquals(0, AirbyteVersion(DEV).versionCompareTo(AirbyteVersion(VERSION_123_PROD)))
  }

  @Test
  fun testGreaterThan() {
    Assertions.assertFalse(AirbyteVersion(VERSION_678_OMEGA).greaterThan(AirbyteVersion(VERSION_678_GAMMA)))
    Assertions.assertFalse(AirbyteVersion(VERSION_678_ALPHA).greaterThan(AirbyteVersion(VERSION_679_ALPHA)))
    Assertions.assertFalse(AirbyteVersion(VERSION_678_ALPHA).greaterThan(AirbyteVersion("6.7.11-alpha")))
    Assertions.assertTrue(AirbyteVersion(VERSION_680_ALPHA).greaterThan(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(AirbyteVersion(VERSION_6110_ALPHA).greaterThan(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertFalse(AirbyteVersion(VERSION_380_ALPHA).greaterThan(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertFalse(AirbyteVersion(VERSION_380_ALPHA).greaterThan(AirbyteVersion("11.7.8-alpha")))
    Assertions.assertFalse(AirbyteVersion(VERSION_123_PROD).greaterThan(AirbyteVersion(DEV)))
    Assertions.assertFalse(AirbyteVersion(DEV).greaterThan(AirbyteVersion(VERSION_123_PROD)))
  }

  @Test
  fun testLessThan() {
    Assertions.assertFalse(AirbyteVersion(VERSION_678_OMEGA).lessThan(AirbyteVersion(VERSION_678_GAMMA)))
    Assertions.assertTrue(AirbyteVersion(VERSION_678_ALPHA).lessThan(AirbyteVersion(VERSION_679_ALPHA)))
    Assertions.assertTrue(AirbyteVersion(VERSION_678_ALPHA).lessThan(AirbyteVersion("6.7.11-alpha")))
    Assertions.assertFalse(AirbyteVersion(VERSION_680_ALPHA).lessThan(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertFalse(AirbyteVersion(VERSION_6110_ALPHA).lessThan(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(AirbyteVersion(VERSION_380_ALPHA).lessThan(AirbyteVersion(VERSION_678_ALPHA)))
    Assertions.assertTrue(AirbyteVersion(VERSION_380_ALPHA).lessThan(AirbyteVersion("11.7.8-alpha")))
    Assertions.assertFalse(AirbyteVersion(VERSION_123_PROD).lessThan(AirbyteVersion(DEV)))
    Assertions.assertFalse(AirbyteVersion(DEV).lessThan(AirbyteVersion(VERSION_123_PROD)))
  }

  @Test
  fun testInvalidVersions() {
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { AirbyteVersion("") })
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { AirbyteVersion("0.6") })
  }

  @Test
  fun testSerialize() {
    Assertions.assertEquals(DEV, AirbyteVersion(DEV).serialize())

    val nonDevVersion = "0.1.2-alpha"
    Assertions.assertEquals(nonDevVersion, AirbyteVersion(nonDevVersion).serialize())
  }

  @Test
  fun testCheckOnlyPatchVersion() {
    Assertions.assertFalse(AirbyteVersion(VERSION_678).checkOnlyPatchVersionIsUpdatedComparedTo(AirbyteVersion(VERSION_678)))
    Assertions.assertFalse(AirbyteVersion("6.9.8").checkOnlyPatchVersionIsUpdatedComparedTo(AirbyteVersion("6.8.9")))
    Assertions.assertFalse(AirbyteVersion("7.7.8").checkOnlyPatchVersionIsUpdatedComparedTo(AirbyteVersion("6.7.11")))
    Assertions.assertFalse(AirbyteVersion("6.6.9").checkOnlyPatchVersionIsUpdatedComparedTo(AirbyteVersion(VERSION_678)))
    Assertions.assertFalse(AirbyteVersion("6.7.11").checkOnlyPatchVersionIsUpdatedComparedTo(AirbyteVersion("7.7.8")))
    Assertions.assertTrue(AirbyteVersion("6.7.9").checkOnlyPatchVersionIsUpdatedComparedTo(AirbyteVersion(VERSION_678)))
  }

  companion object {
    private const val VERSION_678 = "6.7.8"
    private const val VERSION_678_OMEGA = "6.7.8-omega"
    private const val VERSION_678_ALPHA = "6.7.8-alpha"
    private const val VERSION_678_GAMMA = "6.7.8-gamma"
    private const val VERSION_679_ALPHA = "6.7.9-alpha"
    private const val VERSION_680_ALPHA = "6.8.0-alpha"
    private const val VERSION_6110_ALPHA = "6.11.0-alpha"
    private const val VERSION_123_PROD = "1.2.3-prod"
    private const val DEV = "dev"
    private const val VERSION_380_ALPHA = "3.8.0-alpha"
  }
}
