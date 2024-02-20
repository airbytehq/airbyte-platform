import defaults from "lodash/defaults";
import { useCallback, useState } from "react";

type SetFilterValue<T> = <FilterName extends keyof T, FilterType extends T[FilterName]>(
  filterName: FilterName,
  filterValue: FilterType
) => void;

type SetFilters<T> = (filters: T) => void;

export const useFilters = <T>(defaultValues: T): [T, SetFilterValue<T>, SetFilters<T>] => {
  const [filterValues, setFilterValues] = useState<T>(() => defaults({}, defaultValues));

  const setFilterValue = useCallback<SetFilterValue<T>>(
    (filterName, filterValue) => {
      setFilterValues((prevValues) => {
        return { ...prevValues, [filterName]: filterValue };
      });
    },
    [setFilterValues]
  );
  return [filterValues, setFilterValue, setFilterValues];
};
