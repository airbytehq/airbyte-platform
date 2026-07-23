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
import { isPrimaryKey, checkCursorAndPKRequirements } from "../../utils";

export interface FieldPKCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const FieldPKCell: React.FC<FieldPKCellProps> = ({ row }) => {
  const { streamNode, field } = row.original;

  if (!field || SyncSchemaFieldObject.isNestedField(field) || !streamNode?.config?.selected) {
    return null;
  }

  const { pkRequired, shouldDefinePk } = checkCursorAndPKRequirements(streamNode.config, streamNode.stream!);

  // PK is not supported
  if (!pkRequired) {
    return null;
  }

  const primaryKeyDefined = shouldDefinePk && SyncSchemaFieldObject.isPrimitive(field);
  const isSelectedPrimaryKey = isPrimaryKey(streamNode.config, field.path);

  const pkLabel = isSelectedPrimaryKey ? (
    <FlexContainer gap="sm">
      <Icon type="keyCircle" color="primary" size="sm" />
      <Text color="blue">
        <FormattedMessage id="form.primaryKey.label" />
      </Text>
    </FlexContainer>
  ) : null;

  return (
    <FlexContainer alignItems="center" justifyContent="flex-start" data-testid="field-pk-cell">
      {isSelectedPrimaryKey && !primaryKeyDefined ? (
        <Tooltip placement="bottom" control={pkLabel}>
          <FormattedMessage id="form.field.sourceDefinedPK" />
          <TooltipLearnMoreLink url={links.sourceDefinedPKLink} />
        </Tooltip>
      ) : (
        pkLabel
      )}
    </FlexContainer>
  );
};
