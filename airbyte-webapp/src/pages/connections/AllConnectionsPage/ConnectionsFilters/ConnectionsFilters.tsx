import { Listbox } from "@headlessui/react";
import React, { useMemo, useState } from "react";
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
import { Tag, WebBackendConnectionListFiltersStatusesItem } from "core/api/types/AirbyteClient";
import { naturalComparatorBy } from "core/utils/objects";
import { useHeadlessUiOnClose } from "core/utils/useHeadlessUiOnClose";

import styles from "./ConnectionsFilters.module.scss";
import {
  useAvailableSourceOptions,
  stateFilterOptions,
  statusFilterOptions,
  useAvailableDestinationOptions,
} from "./filterOptions";

export interface FilterValues {
  search: string;
  status: WebBackendConnectionListFiltersStatusesItem | null;
  state: "active" | "inactive" | null;
  source: string | null;
  destination: string | null;
}

interface ConnectionsTableFiltersProps {
  searchFilter: string;
  setSearchFilter: (value: string) => void;
  filterValues: FilterValues;
  setFilterValue: (key: keyof FilterValues, value: string | null) => void;
  resetFilters: () => void;
  tagFilters: string[];
  setTagFilters: (selectedTagIds: string[]) => void;
}

export const ConnectionsFilters: React.FC<ConnectionsTableFiltersProps> = ({
  searchFilter,
  setSearchFilter,
  filterValues,
  setFilterValue,
  resetFilters,
  tagFilters,
  setTagFilters,
}) => {
  const availableSourceOptions = useAvailableSourceOptions();
  const availableDestinationOptions = useAvailableDestinationOptions();

  const hasAnyFilterSelected =
    !!filterValues.status ||
    !!filterValues.source ||
    !!filterValues.state ||
    !!filterValues.destination ||
    tagFilters.length > 0 ||
    searchFilter;

  return (
    <Box px="lg" pt="lg">
      <FlexContainer justifyContent="flex-start" direction="column">
        <FlexItem grow>
          <SearchInput value={searchFilter} onChange={setSearchFilter} debounceTimeout={300} />
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
          <TagFilterDropdown selectedTagIds={tagFilters} setSelectedTagIds={(values) => setTagFilters(values)} />
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
  const [selectedTagIdsOnOpen, setSelectedTagIdsOnOpen] = useState(selectedTagIds);
  const workspaceId = useCurrentWorkspaceId();
  const tags = useTagsList(workspaceId);

  const alphabeticallySortedTags = useMemo(() => tags.sort(naturalComparatorBy((tag) => tag.name)), [tags]);

  // For better UX, the originally selected tags should always be at the top of the list
  const sortedTags = useMemo(() => {
    const selectedTagsSet = new Set(selectedTagIdsOnOpen);

    const topSection: Tag[] = [];
    const bottomSection: Tag[] = [];

    alphabeticallySortedTags.forEach((tag) =>
      selectedTagsSet.has(tag.tagId) ? topSection.push(tag) : bottomSection.push(tag)
    );

    return [...topSection, ...bottomSection];
  }, [selectedTagIdsOnOpen, alphabeticallySortedTags]);

  const onCloseListbox = () => {
    setSelectedTagIdsOnOpen(selectedTagIds);
  };

  const { targetRef } = useHeadlessUiOnClose(onCloseListbox);

  return (
    <Listbox
      as="div"
      multiple
      onChange={setSelectedTagIds}
      value={selectedTagIds}
      ref={targetRef}
      data-testid="connection-list-tags-filter"
    >
      <FloatLayout>
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
          {sortedTags.map((tag) => (
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
