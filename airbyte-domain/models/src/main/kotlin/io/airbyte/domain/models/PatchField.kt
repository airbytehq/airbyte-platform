/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

/**
* A tri‑state wrapper used to represent an optional patch operation on a field of type [T].
*
* There are three possible states:
*  1. [Absent]    – the client did _not_ send this field; the existing value should be left unchanged.
*  2. [Present]   – the client _did_ send this field:
*       • if [Present.value] is non‑null, the field should be set to that value;
*       • if [Present.value] is `null` (only possible when `T` itself is nullable), the field should be cleared to `null`.
*
* The generic parameter `T` may be nullable or non‑nullable:
*  - If `T` is non‑nullable, you cannot create `Present(null)`—attempting to do so will fail at compile time.
*  - If `T` is nullable (e.g. `OffsetDateTime?`), then `Present(null)` is legal and represents an explicit clear.
*
* Marked `out T` (covariant) so that a `PatchField<Derived>` can be used where a `PatchField<Base>` is expected.
*/
sealed class PatchField<out T> {
  /**
   * Represents “no change” – the field was omitted from the patch payload.
   *
   * Use this as the default for any patchable constructor parameter:
   * ```kotlin
   * data class FooPatch(
   *   val name: PatchField<String> = Absent,
   *   val count: PatchField<Int>    = Absent
   * )
   * ```
   */
  object Absent : PatchField<Nothing>()

  /**
   * Represents “set this field to [value]”:
   *  - If `T` is non‑nullable, `[value]` cannot be null.
   *  - If `T` is nullable, `[value]` may be null to explicitly clear the field.
   *
   * @param value the new value to assign (possibly `null` if `T` is nullable)
   */
  data class Present<out T>(
    val value: T,
  ) : PatchField<T>()

  companion object {
    /**
     * Extension function to apply this patch to an existing value.
     *
     * @param currentValue the current value
     * @return currentValue if [Absent], otherwise the wrapped value
     */
    fun <T> PatchField<T>.applyTo(currentValue: T): T =
      when (this) {
        is Absent -> currentValue
        is Present -> value
      }

    /**
     * Wrap a non‑nullable value into a Present patch.
     */
    @JvmStatic
    @JvmName("toPatch")
    fun <T : Any> T.toPatch(): PatchField<T> = Present(this)

    /**
     * Wrap a nullable value into a Present patch.
     * Allows explicit null-clearing for nullable targets.
     */
    @JvmStatic
    @JvmName("toPatchNullable")
    fun <T> T?.toPatch(): PatchField<T?> = Present(this)
  }
}
