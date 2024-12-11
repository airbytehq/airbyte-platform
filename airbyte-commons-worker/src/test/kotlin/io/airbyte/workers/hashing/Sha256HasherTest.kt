package io.airbyte.workers.hashing

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class Sha256HasherTest {
  private val hasher = Sha256Hasher()

  @ParameterizedTest
  @MethodSource("hashTestProvider")
  fun test(
    input: String,
    expected: String,
  ) {
    assertEquals(expected, hasher.hash(input))
  }

  @ParameterizedTest
  @MethodSource("saltedHashTestProvider")
  fun testWithSalt(
    input: String,
    expected: String,
    salt: String,
  ) {
    assertEquals(expected, hasher.hash(input, salt))
  }

  companion object {
    @JvmStatic
    fun hashTestProvider(): Stream<Arguments> {
      return Stream.of(
        Arguments.of("token1", "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de"),
        Arguments.of("asdfasdf", "2413fb3709b05939f04cf2e92f7d0897fc2596f9ad0b8a9ea855c7bfebaae892"),
        Arguments.of("differentstring", "bccfc5cb1ca22da8a6912fc1462d3c52e8fa2b8d1313077c092f09b9ae61925b"),
      )
    }

    @JvmStatic
    fun saltedHashTestProvider(): Stream<Arguments> {
      return Stream.of(
        Arguments.of("token1", "fe1897ce53efcc5f79380957459cba5cc788f0713a605e71b57c5d3031f841a6", "randomsalt"),
        Arguments.of("asdfasdf", "46f7080c240fe300c78b81631a9d127adabac1bbdd6556bf39fac2f62bda5bd0", "randomsalt"),
        Arguments.of("differentstring", "4dba3dce7d92f669f1bc4384402a8a16e6ecd2fa4b228560e629b16fbc2a9ebd", "randomsalt"),
      )
    }
  }
}
