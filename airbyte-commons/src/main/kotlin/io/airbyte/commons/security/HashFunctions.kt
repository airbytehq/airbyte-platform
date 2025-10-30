/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.security

import org.apache.commons.codec.digest.MurmurHash3
import java.security.MessageDigest

/**
 * Computes the MD5 hash of the byte array and returns the result as a hexadecimal string.
 *
 * @return the MD5 hash of the byte array represented as a hexadecimal string.
 */
fun ByteArray.md5(): String = MessageDigest.getInstance("MD5").digest(this).toHexString()

/**
 * Computes the 32-bit Murmur3 hash of the ByteArray and returns its hexadecimal string representation.
 *
 * @return A hexadecimal string representing the 32-bit Murmur3 hash of the ByteArray.
 */
fun ByteArray.murmur332(): String = MurmurHash3.hash32x86(this).toHexString()

/**
 * Computes the SHA-256 hash of the current byte array and returns its hexadecimal string representation.
 *
 * @return the hexadecimal string representation of the SHA-256 hash for the byte array
 */
fun ByteArray.sha256(): String = MessageDigest.getInstance("SHA-256").digest(this).toHexString()
