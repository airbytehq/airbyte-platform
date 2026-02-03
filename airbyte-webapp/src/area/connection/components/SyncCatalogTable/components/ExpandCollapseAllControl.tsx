import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

interface ExpandCollapseAllControlProps {
  isAllRowsExpanded: boolean;
  toggleAllRowsExpanded: (expanded: boolean) => void;
}

export const ExpandCollapseAllControl: React.FC<ExpandCollapseAllControlProps> = ({
  isAllRowsExpanded,
  toggleAllRowsExpanded,
}) => (
  <Tooltip
    placement="top"
    control={
      <Button
        icon={isAllRowsExpanded ? "collapseAll" : "expandAll"}
        variant="secondary"
        type="button"
        data-testid="expand-collapse-all-streams-btn"
        onClick={() => toggleAllRowsExpanded(!isAllRowsExpanded)}
      />
    }
  >
    <FormattedMessage id={isAllRowsExpanded ? "tables.collapseAll" : "tables.expandAll"} />
  </Tooltip>
);
