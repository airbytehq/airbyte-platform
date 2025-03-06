/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.enums;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Common code for operations on a {@link Enum}.
 */
public class Enums {

  /**
   * Convert an enum value from the type of one enum to the same value in the type of another enum.
   * This is useful for converting from enums that are conceptually the same but are different java
   * types. e.g. if the same model has multiple generated java types, this method allows you to
   * convert between them.
   *
   * @param ie input enum value
   * @param oe class of output enum
   * @param <T1> type of input enum
   * @param <T2> type of output enum
   * @return enum value as type of output enum
   */
  public static <T1 extends Enum<T1>, T2 extends Enum<T2>> T2 convertTo(final T1 ie, final Class<T2> oe) {
    if (ie == null) {
      return null;
    }

    return Enum.valueOf(oe, ie.name());
  }

  /**
   * Convert a list of enum values from the type onf one enum to the same value in the type of another
   * enum.
   *
   * @param ies list of input enum values
   * @param oe class of output enum
   * @param <T1> type of input enum
   * @param <T2> type of output enum
   * @return enum values as type of output enum
   */
  public static <T1 extends Enum<T1>, T2 extends Enum<T2>> List<T2> convertListTo(final List<T1> ies, final Class<T2> oe) {
    return ies
        .stream()
        .map(ie -> convertTo(ie, oe))
        .collect(Collectors.toList());
  }

  /**
   * Test if two enums are compatible to be converted between. To be compatible they must have the
   * same values.
   *
   * @param c1 class of enum 1
   * @param c2 class of enum 2
   * @param <T1> type of enum 1
   * @param <T2> type of enum 2
   * @return true if compatible. otherwise, false.
   */
  public static <T1 extends Enum<T1>, T2 extends Enum<T2>> boolean isCompatible(final Class<T1> c1,
                                                                                final Class<T2> c2) {
    Preconditions.checkArgument(c1.isEnum());
    Preconditions.checkArgument(c2.isEnum());
    return c1.getEnumConstants().length == c2.getEnumConstants().length
        && Sets.difference(
            Arrays.stream(c1.getEnumConstants()).map(Enum::name).collect(Collectors.toSet()),
            Arrays.stream(c2.getEnumConstants()).map(Enum::name).collect(Collectors.toSet()))
            .isEmpty();
  }

  private static final Map<Class<?>, Map<String, ?>> NORMALIZED_ENUMS = Maps.newConcurrentMap();

  /**
   * Convert a string to its values as an enum.
   *
   * @param value string to convert
   * @param enumClass target type of enum
   * @param <T> type of enum
   * @return value as enum value
   */
  @SuppressWarnings("unchecked")
  public static <T extends Enum<T>> Optional<T> toEnum(final String value, final Class<T> enumClass) {
    Preconditions.checkArgument(enumClass.isEnum());

    if (!NORMALIZED_ENUMS.containsKey(enumClass)) {
      final T[] values = enumClass.getEnumConstants();
      final Map<String, T> mappings = Maps.newHashMapWithExpectedSize(values.length);
      for (final T t : values) {
        mappings.put(normalizeName(t.name()), t);
      }
      NORMALIZED_ENUMS.put(enumClass, mappings);
    }

    return Optional.ofNullable((T) NORMALIZED_ENUMS.get(enumClass).get(normalizeName(value)));
  }

  private static String normalizeName(final String name) {
    return name.toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
  }

}
