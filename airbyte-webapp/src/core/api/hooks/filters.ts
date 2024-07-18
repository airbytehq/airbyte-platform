import isEqual from "lodash/isEqual";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

type SetFilterValue<T> = <FilterName extends keyof T, FilterType extends T[FilterName]>(
  filterName: FilterName,
  filterValue: FilterType
) => void;

type IsDefault = boolean;
type ResetFilters = () => void;

export const useFilters = <T extends Record<keyof T, string | null>>(
  defaultValues: T
): [T, SetFilterValue<T>, ResetFilters, IsDefault] => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [filterValues, setFilterValues] = useState<T>(() => {
    const valuesFromSearchParams = Object.keys(defaultValues).reduce<Record<string, string>>((acc, filterName) => {
      const paramValue = searchParams.get(filterName);
      if (paramValue) {
        acc[filterName] = paramValue;
      }
      return acc;
    }, {});

    return {
      ...defaultValues,
      ...valuesFromSearchParams,
    };
  });

  const setFilterValue = useCallback<SetFilterValue<T>>((filterName, filterValue) => {
    setFilterValues((prevValues) => {
      return { ...prevValues, [filterName]: filterValue };
    });
  }, []);

  // Use a ref so that if callers instantiate defaultValues inline, it doesn't cause this useEffect
  // to be executed on every render cycle.
  // We also don't expect this value to change, since it is a default.
  const defaultValuesRef = useRef(defaultValues);
  defaultValuesRef.current = defaultValues;

  useEffect(() => {
    // combine existing search params with filter values
    const nextSearchParams = {
      ...Object.fromEntries(searchParams),
      ...filterValues,
    };

    // filter out filter values that match their defaults
    const filteredSearchParams = Object.fromEntries(
      Object.entries(nextSearchParams).filter(([key, value]) => value !== defaultValuesRef.current[key as keyof T])
    );

    // this useEffect can trigger itself forever, so check for sameness
    if (!filtersAreEqual(filteredSearchParams, searchParams)) {
      setSearchParams(filteredSearchParams, { replace: true });
    }
  }, [searchParams, filterValues, setSearchParams, defaultValues]);

  const isDefault = useMemo(() => isEqual(filterValues, defaultValuesRef.current), [filterValues]);

  const resetFilters = useCallback(() => {
    setFilterValues(defaultValuesRef.current);
  }, []);

  return [filterValues, setFilterValue, resetFilters, isDefault];
};

function filtersAreEqual(newFilters: Record<string, unknown>, existingParams: URLSearchParams): boolean {
  const aSize = Object.keys(newFilters).length;
  const bSize = existingParams.size;
  return aSize === bSize && Object.keys(newFilters).every((key) => newFilters[key] === existingParams.get(key));
}
