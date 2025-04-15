import { ColumnFiltersState } from "@tanstack/react-table";
import React, { FC } from "react";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { ExpandCollapseAllControl } from "./ExpandCollapseAllControl";
import { FormControls } from "./FormControls";
import { RefreshSchemaControl } from "./RefreshSchemaControl";
import { FilterTabId, StreamsFilterTabs } from "./StreamsFilterTabs";
import styles from "../SyncCatalogTable.module.scss";

interface SearchAndFilterControlsProps {
  filtering: string;
  setFiltering: (value: string) => void;
  isAllStreamRowsExpanded: boolean;
  toggleAllStreamRowsExpanded: (expanded: boolean) => void;
  columnFilters: ColumnFiltersState;
  onTabSelect: (tabId: FilterTabId) => void;
}

export const SearchAndFilterControls: FC<SearchAndFilterControlsProps> = ({
  filtering,
  setFiltering,
  isAllStreamRowsExpanded,
  toggleAllStreamRowsExpanded,
  columnFilters,
  onTabSelect,
}) => {
  const { formatMessage } = useIntl();
  const { mode } = useConnectionFormService();

  return (
    <>
      <Box p="md" pl="xl" pr="xl" className={styles.stickyControlsContainer}>
        <FlexContainer alignItems="center" justifyContent="space-between">
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
