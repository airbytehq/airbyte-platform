import { Listbox } from "@headlessui/react";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { CheckBox } from "components/ui/CheckBox";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { SearchInput } from "components/ui/SearchInput";
import { TagBadge } from "components/ui/TagBadge";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useTagsList } from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

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
  tagFilters: string[];
  setTagFilters: (selectedTagIds: string[]) => void;
}

export const ConnectionsFilters: React.FC<ConnectionsTableFiltersProps> = ({
  connections,
  searchFilter,
  setSearchFilter,
  filterValues,
  setFilterValue,
  resetFilters,
  tagFilters,
  setTagFilters,
}) => {
  const isConnectionTagsEnabled = useExperiment("connection.tags");

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
    tagFilters.length > 0 ||
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
          {isConnectionTagsEnabled && (
            <FlexItem>
              <TagFilterDropdown selectedTagIds={tagFilters} setSelectedTagIds={(values) => setTagFilters(values)} />
            </FlexItem>
          )}
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

interface TagFilterDropdownProps {
  selectedTagIds: string[];
  setSelectedTagIds: (selectedTagIds: string[]) => void;
}

const TagFilterDropdown: React.FC<TagFilterDropdownProps> = ({ selectedTagIds, setSelectedTagIds }) => {
  const workspaceId = useCurrentWorkspaceId();
  const tags = useTagsList(workspaceId);

  return (
    <Listbox multiple onChange={setSelectedTagIds} value={selectedTagIds}>
      <FloatLayout
        shift={5} // $spacing-sm
      >
        <ListboxButton>
          <FlexContainer gap="sm" alignItems="center">
            {selectedTagIds.length === 0 && (
              <Text bold color="grey">
                <FormattedMessage id="connection.tags.title" />
              </Text>
            )}
            {selectedTagIds.length > 0 &&
              selectedTagIds?.map((tagId) => {
                const tag = tags.find((t) => t.tagId === tagId);
                return tag ? <TagBadge key={tagId} color={tag.color} text={tag.name} /> : null;
              })}
          </FlexContainer>
        </ListboxButton>
        <ListboxOptions>
          {tags.length === 0 && (
            <Box p="md">
              <Text color="grey" italicized>
                <FormattedMessage id="connection.tags.empty" />
              </Text>
            </Box>
          )}
          {tags.map((tag) => (
            <ListboxOption key={tag.tagId} value={tag.tagId}>
              {({ selected }) => (
                <Box p="md">
                  <FlexContainer alignItems="center" as="span">
                    <CheckBox checked={selected} readOnly />
                    <TagBadge color={tag.color} text={tag.name} />
                  </FlexContainer>
                </Box>
              )}
            </ListboxOption>
          ))}
        </ListboxOptions>
      </FloatLayout>
    </Listbox>
  );
};
