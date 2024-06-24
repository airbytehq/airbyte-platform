import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

type SetFilterValue<T> = <FilterName extends keyof T, FilterType extends T[FilterName]>(
  filterName: FilterName,
  filterValue: FilterType
) => void;

type SetFilters<T> = (filters: T) => void;

export const useFilters = <T extends object>(defaultValues: T): [T, SetFilterValue<T>, SetFilters<T>, boolean] => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [isInitialState, setIsInitialState] = useState<boolean>(true);

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

  const setFilterValue = useCallback<SetFilterValue<T>>(
    (filterName, filterValue) => {
      if (isInitialState) {
        setIsInitialState(false);
      }
      setFilterValues((prevValues) => {
        return { ...prevValues, [filterName]: filterValue };
      });
    },
    [isInitialState]
  );

  useEffect(() => {
    // combine existing search params with filter values
    const nextSearchParams = {
      ...searchParams,
      ...filterValues,
    };

    // filter out filter values that match their defaults
    const filteredSearchParams = Object.fromEntries(
      Object.entries(nextSearchParams).filter(([key, value]) => value !== defaultValues[key as keyof T])
    );

    // this useEffect can trigger itself forever, so check for sameness
    if (!filtersAreEqual(filteredSearchParams, searchParams)) {
      // @ts-expect-error TS thinks Object.entries returns items from the prototype chain
      setSearchParams(filteredSearchParams, { replace: true });
    }
  }, [searchParams, filterValues, setSearchParams, defaultValues]);

  return [filterValues, setFilterValue, setFilterValues, isInitialState];
};

function filtersAreEqual(newFilters: Record<string, unknown>, existingParams: URLSearchParams): boolean {
  const aSize = Object.keys(newFilters).length;
  const bSize = existingParams.size;
  return aSize === bSize && Object.keys(newFilters).every((key) => newFilters[key] === existingParams.get(key));
}
