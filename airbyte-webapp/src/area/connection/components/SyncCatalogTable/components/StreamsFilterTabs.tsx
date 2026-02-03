import { ColumnFilter } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { ButtonTab, Tabs } from "components/ui/Tabs";

export type FilterTabId = "all" | "enabledStreams" | "disabledStreams";

interface StreamsFilterTabsProps {
  columnFilters: ColumnFilter[];
  onTabSelect: (tabId: FilterTabId) => void;
}

export const StreamsFilterTabs: React.FC<StreamsFilterTabsProps> = ({ columnFilters, onTabSelect }) => (
  <Tabs>
    <ButtonTab
      id="all"
      name={<FormattedMessage id="form.streams.all" />}
      isActive={columnFilters.filter((column) => column.id === "stream.selected").length === 0}
      onSelect={(id) => onTabSelect(id as FilterTabId)}
    />
    <ButtonTab
      id="enabledStreams"
      name={<FormattedMessage id="form.streams.enabledStreams" />}
      isActive={columnFilters.filter((column) => column.id === "stream.selected")?.[0]?.value === true}
      onSelect={(id) => onTabSelect(id as FilterTabId)}
    />
    <ButtonTab
      id="disabledStreams"
      name={<FormattedMessage id="form.streams.disabledStreams" />}
      isActive={columnFilters.filter((column) => column.id === "stream.selected")?.[0]?.value === false}
      onSelect={(id) => onTabSelect(id as FilterTabId)}
    />
  </Tabs>
);
