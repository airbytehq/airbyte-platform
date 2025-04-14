/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.enums

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

private enum class A { ZERO, ONE, TWO }

private enum class B { ONE, TWO, THREE }

private enum class C { ONE, TWO, THREE }

// TODO: rename once the old java enums class is no more
class EnumsKtTest {
  @Test
  fun `verify convertTo`() {
    assertEquals(B.ONE, A.ONE.convertTo<B>())
  }

  @Test
  fun `verify convertTo mismatch`() {
    assertThrows<IllegalArgumentException> { A.ZERO.convertTo<B>() }
  }

  @Test
  fun `verify list convertTo`() {
    val input = listOf(A.ONE, A.TWO)
    val expected = listOf(B.ONE, B.TWO)

    assertEquals(expected, input.convertTo<B>())
  }

  @Test
  fun `verify list convertTo mismatch`() {
    assertThrows<IllegalArgumentException> {
      listOf(A.ZERO, A.TWO).convertTo<B>()
    }
  }

  @Test
  fun `verify toEnum`() {
    assertEquals(A.ONE, "ONE".toEnum<A>())
    assertNull("DNE".toEnum<A>())
  }
}
