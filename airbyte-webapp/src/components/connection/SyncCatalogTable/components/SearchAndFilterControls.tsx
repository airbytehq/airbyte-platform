import { ColumnFiltersState } from "@tanstack/react-table";
import React, { FC } from "react";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { SearchInput } from "components/ui/SearchInput";

import { useFormMode } from "core/services/ui/FormModeContext";

import { ExpandCollapseAllControl } from "./ExpandCollapseAllControl";
import { FormControls } from "./FormControls";
import { RefreshSchemaControl } from "./RefreshSchemaControl";
import { FilterTabId, StreamsFilterTabs } from "./StreamsFilterTabs";
import styles from "../SyncCatalogTable.module.scss";

export const STREAMS_ONLY = 1;
export const STREAMS_AND_FIELDS = 100;

interface SearchAndFilterControlsProps {
  filtering: string;
  setFiltering: (value: string) => void;
  filteringDepth: number;
  setFilteringDepth: (value: number) => void;
  isAllStreamRowsExpanded: boolean;
  toggleAllStreamRowsExpanded: (expanded: boolean) => void;
  columnFilters: ColumnFiltersState;
  onTabSelect: (tabId: FilterTabId) => void;
}

export const SearchAndFilterControls: FC<SearchAndFilterControlsProps> = ({
  filtering,
  setFiltering,
  filteringDepth,
  setFilteringDepth,
  isAllStreamRowsExpanded,
  toggleAllStreamRowsExpanded,
  columnFilters,
  onTabSelect,
}) => {
  const { formatMessage } = useIntl();
  const { mode } = useFormMode();

  const filterDepthOptions = [
    { label: formatMessage({ id: "form.filteringDepth.all" }), value: STREAMS_AND_FIELDS },
    { label: formatMessage({ id: "form.filteringDepth.streams" }), value: STREAMS_ONLY },
  ];

  return (
    <>
      <Box p="md" pl="xl" pr="xl" className={styles.stickyControlsContainer}>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <FlexContainer alignItems="center" gap="lg">
            <SearchInput
              value={filtering}
              placeholder={formatMessage({
                id: "form.streamOrFieldSearch",
              })}
              containerClassName={styles.searchInputContainer}
              onChange={(e) => setFiltering(e.target.value)}
              onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
                // We do not want to submit the connection form when pressing Enter in the search field
                e.key === "Enter" && e.preventDefault();
              }}
              data-testid="sync-catalog-search"
            />
            <ListBox
              options={filterDepthOptions}
              selectedValue={filteringDepth}
              onSelect={setFilteringDepth}
              data-testid="sync-catalog-filter-depth"
            />
          </FlexContainer>
          <FlexContainer>
            <FlexContainer justifyContent="flex-end" alignItems="center" direction="row" gap="lg">
              {mode === "create" ? (
                <>
                  <RefreshSchemaControl />
                  <ExpandCollapseAllControl
                    isAllRowsExpanded={isAllStreamRowsExpanded}
                    toggleAllRowsExpanded={toggleAllStreamRowsExpanded}
                  />
                </>
              ) : (
                <>
                  <FormControls>
                    <RefreshSchemaControl />
                  </FormControls>
                  <ExpandCollapseAllControl
                    isAllRowsExpanded={isAllStreamRowsExpanded}
                    toggleAllRowsExpanded={toggleAllStreamRowsExpanded}
                  />
                </>
              )}
            </FlexContainer>
          </FlexContainer>
        </FlexContainer>
      </Box>
      <Box pl="xl" className={styles.stickyTabsContainer}>
        <StreamsFilterTabs columnFilters={columnFilters} onTabSelect={onTabSelect} />
      </Box>
    </>
  );
};
