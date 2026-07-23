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
import { Separator } from "components/ui/Separator";
import { TagBadge } from "components/ui/TagBadge";
import { Text } from "components/ui/Text";

import { BurstTag } from "area/connection/components/EntityTable/BurstTag";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useTagsList } from "core/api";
import { Tag, WebBackendConnectionListFiltersStatusesItem } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
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
  burstFilter: boolean;
  setBurstFilter: (value: boolean) => void;
}

export const ConnectionsFilters: React.FC<ConnectionsTableFiltersProps> = ({
  searchFilter,
  setSearchFilter,
  filterValues,
  setFilterValue,
  resetFilters,
  tagFilters,
  setTagFilters,
  burstFilter,
  setBurstFilter,
}) => {
  const availableSourceOptions = useAvailableSourceOptions();
  const availableDestinationOptions = useAvailableDestinationOptions();
  const isOnDemandCapacityEnabled = useFeature(FeatureItem.OnDemandCapacity);

  const hasAnyFilterSelected =
    !!filterValues.status ||
    !!filterValues.source ||
    !!filterValues.state ||
    !!filterValues.destination ||
    tagFilters.length > 0 ||
    burstFilter ||
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
          <TagFilterDropdown
            selectedTagIds={tagFilters}
            setSelectedTagIds={(values) => setTagFilters(values)}
            burstFilter={burstFilter}
            setBurstFilter={setBurstFilter}
            showBurstOption={isOnDemandCapacityEnabled}
          />
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
  burstFilter: boolean;
  setBurstFilter: (value: boolean) => void;
  showBurstOption: boolean;
}

const BURST_FILTER_VALUE = "__burst__";

const TagFilterDropdown: React.FC<TagFilterDropdownProps> = ({
  selectedTagIds,
  setSelectedTagIds,
  burstFilter,
  setBurstFilter,
  showBurstOption,
}) => {
  const [selectedTagIdsOnOpen, setSelectedTagIdsOnOpen] = useState(selectedTagIds);
  const workspaceId = useCurrentWorkspaceId();
  const tags = useTagsList(workspaceId);

  const alphabeticallySortedTags = useMemo(() => tags.sort(naturalComparatorBy((tag) => tag.name)), [tags]);

  // Combine burst filter with tag IDs for the Listbox value
  const listboxValue = useMemo(() => {
    const values = [...selectedTagIds];
    if (burstFilter) {
      values.unshift(BURST_FILTER_VALUE);
    }
    return values;
  }, [selectedTagIds, burstFilter]);

  // Handle changes from the Listbox, separating burst filter from tag IDs
  const handleChange = (values: string[]) => {
    const hasBurst = values.includes(BURST_FILTER_VALUE);
    const tagIds = values.filter((v) => v !== BURST_FILTER_VALUE);

    setBurstFilter(hasBurst);
    setSelectedTagIds(tagIds);
  };

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

  const hasNoFilters = selectedTagIds.length === 0 && !burstFilter;

  return (
    <Listbox
      as="div"
      multiple
      onChange={handleChange}
      value={listboxValue}
      ref={targetRef}
      data-testid="connection-list-tags-filter"
    >
      <FloatLayout>
        <ListboxButton>
          <FlexContainer gap="sm" alignItems="center">
            {hasNoFilters && (
              <Text bold color="grey">
                <FormattedMessage id="connection.tags.title" />
              </Text>
            )}
            {burstFilter && <BurstTag />}
            {selectedTagIds.length > 0 &&
              selectedTagIds?.map((tagId) => {
                const tag = tags.find((t) => t.tagId === tagId);
                return tag ? <TagBadge key={tagId} color={tag.color} text={tag.name} /> : null;
              })}
          </FlexContainer>
        </ListboxButton>
        <ListboxOptions>
          {showBurstOption && (
            <>
              <ListboxOption value={BURST_FILTER_VALUE} data-testid="burst-filter-option">
                {({ selected }) => (
                  <Box p="md">
                    <FlexContainer alignItems="center" as="span">
                      <CheckBox checked={selected} readOnly />
                      <BurstTag />
                    </FlexContainer>
                  </Box>
                )}
              </ListboxOption>
              {tags.length > 0 && <Separator />}
            </>
          )}
          {tags.length === 0 && !showBurstOption && (
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
