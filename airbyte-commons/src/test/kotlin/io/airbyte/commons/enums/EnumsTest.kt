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

class EnumsTest {
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

  enum class E1 {
    TEST,
    TEST2,
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class E5 {
    VALUE_1,
    VALUE_TWO,
    value_three,
    value_4,
  }

  @Test
  fun `verify toEnum`() {
    assertEquals(A.ONE, "ONE".toEnum<A>())
    assertNull("DNE".toEnum<A>())

    assertEquals(E1.TEST, "test".toEnum<E1>())
    assertEquals(E5.VALUE_1, "VALUE_1".toEnum<E5>())
    assertEquals(E5.VALUE_1, "value_1".toEnum<E5>())
    assertEquals(E5.VALUE_TWO, "VALUE_TWO".toEnum<E5>())
    assertEquals(E5.VALUE_TWO, "valuetwo".toEnum<E5>())
    assertEquals(E5.VALUE_TWO, "valueTWO".toEnum<E5>())
    assertEquals(E5.VALUE_TWO, "valueTWO$".toEnum<E5>())
    assertEquals(E5.VALUE_TWO, "___valueTWO___".toEnum<E5>())
    assertEquals(E5.value_three, "VALUE_THREE".toEnum<E5>())
    assertEquals(E5.value_4, "VALUE_4".toEnum<E5>())
    assertEquals(null, "VALUE_5".toEnum<E5>())
  }
}
