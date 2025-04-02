import { Row } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { SyncSchemaFieldObject } from "core/domain/catalog";
import { links } from "core/utils/links";

import { SyncCatalogUIModel } from "../../SyncCatalogTable";
import { isCursor, checkCursorAndPKRequirements } from "../../utils";

interface FieldCursorCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const FieldCursorCell: React.FC<FieldCursorCellProps> = ({ row }) => {
  const { streamNode, field } = row.original;

  if (
    !field ||
    SyncSchemaFieldObject.isNestedField(field) ||
    !streamNode?.config?.selected ||
    !streamNode.config ||
    !streamNode.stream
  ) {
    return null;
  }

  const { cursorRequired, shouldDefineCursor } = checkCursorAndPKRequirements(streamNode.config, streamNode.stream);

  // Cursor is not supported
  if (!cursorRequired) {
    return null;
  }

  const cursorDefined = shouldDefineCursor && SyncSchemaFieldObject.isPrimitive(field);
  const isSelectedCursor = isCursor(streamNode.config, field.path);

  const cursorLabel = isSelectedCursor ? (
    <FlexContainer gap="sm">
      <Icon type="cursor" color="primary" size="sm" />
      <Text color="blue">
        <FormattedMessage id="form.cursorField.label" />
      </Text>
    </FlexContainer>
  ) : null;

  return (
    <FlexContainer alignItems="center" justifyContent="flex-start" data-testid="field-cursor-cell">
      {isSelectedCursor && !cursorDefined ? (
        <Tooltip placement="bottom" control={cursorLabel}>
          <FormattedMessage id="form.field.sourceDefinedCursor" />
          <TooltipLearnMoreLink url={links.sourceDefinedCursorLink} />
        </Tooltip>
      ) : (
        cursorLabel
      )}
    </FlexContainer>
  );
};
