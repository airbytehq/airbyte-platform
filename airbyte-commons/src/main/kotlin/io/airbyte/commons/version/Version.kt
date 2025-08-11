/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import java.io.Serializable
import java.util.Objects

/**
 * A semVer Version class that allows "dev" as a version.
 */
@JsonDeserialize(using = VersionDeserializer::class)
@JsonSerialize(using = VersionSerializer::class)
open class Version : Serializable {
  companion object {
    const val DEV_VERSION_PREFIX = "dev"

    /**
     * Compares two Versions to check if they are compatible. Only the major and minor part of the
     * Version is taken into account.
     *
     * @param v1 version to compare
     * @param v2 version to compare
     * @return true if compatible. otherwise, false.
     */
    fun isCompatible(
      v1: Version,
      v2: Version,
    ): Boolean = v1.compatibleVersionCompareTo(v2) == 0

    /**
     * Version string needs to be converted to integer for comparison, because string comparison does
     * not handle version string with different digits correctly. For example:
     * `"11".compare("3") < 0`, while `Integer.compare(11, 3) > 0`.
     */
    private fun compareVersion(
      v1: String,
      v2: String,
    ): Int = Integer.compare(v1.toInt(), v2.toInt())
  }

  private val version: String
  private val major: String?
  private val minor: String?
  private val patch: String?

  constructor(version: String) {
    requireNotNull(version)
    this.version = version
    val parsedVersion =
      version
        .replace("\n", "")
        .trim()
        .split("-")[0]
        .split(".")

    if (isDev()) {
      this.major = null
      this.minor = null
      this.patch = null
    } else {
      require(parsedVersion.size >= 3) { "Invalid version string: $version" }
      this.major = parsedVersion[0]
      this.minor = parsedVersion[1]
      this.patch = parsedVersion[2]
    }
  }

  constructor(major: String, minor: String, patch: String) {
    this.version = "$major.$minor.$patch"
    this.major = major
    this.minor = minor
    this.patch = patch
  }

  fun serialize(): String = version

  fun getMajorVersion(): String? = major

  fun getMinorVersion(): String? = minor

  fun getPatchVersion(): String? = patch

  /**
   * Compares two Versions to check if they are compatible. Only the major and minor part of the
   * Version is taken into account.
   *
   * @param another version to compare
   * @return the value 0 if version == another; a value less than 0 if version < another; and a value
   *         greater than 0 if version > another
   */
  fun compatibleVersionCompareTo(another: Version): Int {
    if (isDev() || another.isDev()) {
      return 0
    }
    val majorDiff = compareVersion(major!!, another.major!!)
    if (majorDiff != 0) {
      return majorDiff
    }
    return compareVersion(minor!!, another.minor!!)
  }

  /**
   * Test if this version is greater than another version.
   *
   * @param other version to compare
   * @return true if this version is greater than the other, otherwise false.
   */
  fun greaterThan(other: Version): Boolean = versionCompareTo(other) > 0

  /**
   * Test if this version is greater than or equal to another version.
   *
   * @param other version to compare
   * @return true if this version is greater than or equal to the other, otherwise false.
   */
  fun greaterThanOrEqualTo(other: Version): Boolean = versionCompareTo(other) >= 0

  /**
   * Test if a provided version is less than another version.
   *
   * @param other version to compare
   * @return true if this version is less than the other, otherwise false.
   */
  fun lessThan(other: Version): Boolean = versionCompareTo(other) < 0

  /**
   * Test if a provided version is less than or equal to another version.
   *
   * @param other version to compare
   * @return true if this version is less than or equal to the other, otherwise false.
   */
  fun lessThanOrEqualTo(other: Version): Boolean = versionCompareTo(other) <= 0

  /**
   * Compares two Versions to check if they are equivalent.
   *
   * @param another version to compare
   * @return the value 0 if version == another; a value less than 0 if version < another; and a value
   *         greater than 0 if version > another
   */
  fun versionCompareTo(another: Version): Int {
    if (isDev() || another.isDev()) {
      return 0
    }
    val majorDiff = compareVersion(major!!, another.major!!)
    if (majorDiff != 0) {
      return majorDiff
    }
    val minorDiff = compareVersion(minor!!, another.minor!!)
    if (minorDiff != 0) {
      return minorDiff
    }
    return compareVersion(patch!!, another.patch!!)
  }

  /**
   * Compares two Versions to check if only the patch version was updated.
   *
   * @param another version to compare
   * @return true if exactly the same version or if the same version except for the patch. otherwise,
   *         false.
   */
  fun checkOnlyPatchVersionIsUpdatedComparedTo(another: Version): Boolean {
    if (isDev() || another.isDev()) {
      return false
    }
    val majorDiff = compareVersion(major!!, another.major!!)
    if (majorDiff != 0) {
      return false
    }
    val minorDiff = compareVersion(minor!!, another.minor!!)
    if (minorDiff != 0) {
      return false
    }
    return compareVersion(patch!!, another.patch!!) > 0
  }

  /**
   * Test if this version is dev.
   *
   * @return true if dev. otherwise, false.
   */
  fun isDev(): Boolean = version.startsWith(DEV_VERSION_PREFIX)

  override fun toString(): String =
    "Version(" +
      "version='$version', " +
      "major='$major', " +
      "minor='$minor', " +
      "patch='$patch'" +
      ")"

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other == null || javaClass != other.javaClass) return false
    val that = other as Version
    return Objects.equals(version, that.version) &&
      Objects.equals(major, that.major) &&
      Objects.equals(minor, that.minor) &&
      Objects.equals(patch, that.patch)
  }

  override fun hashCode(): Int = Objects.hash(version, major, minor, patch)
}
