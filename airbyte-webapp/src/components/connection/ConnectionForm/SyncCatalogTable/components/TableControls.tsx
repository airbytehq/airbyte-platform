import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

interface TableControlsProps {
  isAllRowsExpanded: boolean;
  toggleAllRowsExpanded: (expanded: boolean) => void;
}

export const TableControls: React.FC<TableControlsProps> = ({ isAllRowsExpanded, toggleAllRowsExpanded }) => (
  <>
    <Tooltip
      placement="top"
      control={
        <Button
          variant="secondary"
          icon="rotate"
          onClick={() => {}} // stub
          disabled // temporary
        />
      }
    >
      <FormattedMessage id="connection.updateSchema" />
    </Tooltip>
    <Tooltip
      placement="top"
      control={
        <Button
          icon={isAllRowsExpanded ? "collapseAll" : "expandAll"}
          variant="secondary"
          type="button"
          onClick={() => toggleAllRowsExpanded(!isAllRowsExpanded)}
        />
      }
    >
      <FormattedMessage id={isAllRowsExpanded ? "tables.collapseAll" : "tables.expandAll"} />
    </Tooltip>
  </>
);
