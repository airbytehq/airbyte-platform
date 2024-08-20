import React, { useMemo } from "react";

import { Box } from "components/ui/Box";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { SearchInput } from "components/ui/SearchInput";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import styles from "./ConnectionsFilters.module.scss";
import {
  getAvailableDestinationOptions,
  getAvailableSourceOptions,
  stateFilterOptions,
  statusFilterOptions,
} from "./filterOptions";

export interface FilterValues {
  search: string;
  status: string | null;
  state: string | null;
  source: string | null;
  destination: string | null;
}

interface ConnectionsTableFiltersProps {
  connections: WebBackendConnectionListItem[];
  searchFilter: string;
  setSearchFilter: (value: string) => void;
  filterValues: FilterValues;
  setFilterValue: (key: keyof FilterValues, value: string | null) => void;
  resetFilters: () => void;
}

export const ConnectionsFilters: React.FC<ConnectionsTableFiltersProps> = ({
  connections,
  searchFilter,
  setSearchFilter,
  filterValues,
  setFilterValue,
  resetFilters,
}) => {
  const availableSourceOptions = useMemo(
    () => getAvailableSourceOptions(connections, filterValues.destination),
    [connections, filterValues.destination]
  );
  const availableDestinationOptions = useMemo(
    () => getAvailableDestinationOptions(connections, filterValues.source),
    [connections, filterValues.source]
  );

  const hasAnyFilterSelected =
    !!filterValues.status ||
    !!filterValues.source ||
    !!filterValues.state ||
    !!filterValues.destination ||
    searchFilter;

  return (
    <Box p="lg">
      <FlexContainer justifyContent="flex-start" direction="column">
        <FlexItem grow>
          <SearchInput value={searchFilter} onChange={({ target: { value } }) => setSearchFilter(value)} />
        </FlexItem>
        <FlexContainer gap="sm" alignItems="center">
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={stateFilterOptions}
              selectedValue={filterValues.state}
              onSelect={(value) => setFilterValue("state", value)}
            />
          </FlexItem>
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={statusFilterOptions}
              selectedValue={filterValues.status}
              onSelect={(value) => setFilterValue("status", value)}
            />
          </FlexItem>
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionsMenuClassName={styles.filterOptionsMenu}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={availableSourceOptions}
              selectedValue={filterValues.source}
              onSelect={(value) => setFilterValue("source", value)}
            />
          </FlexItem>
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={availableDestinationOptions}
              selectedValue={filterValues.destination}
              onSelect={(value) => setFilterValue("destination", value)}
            />
          </FlexItem>
          {hasAnyFilterSelected && (
            <FlexItem>
              <ClearFiltersButton onClick={resetFilters} />
            </FlexItem>
          )}
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
