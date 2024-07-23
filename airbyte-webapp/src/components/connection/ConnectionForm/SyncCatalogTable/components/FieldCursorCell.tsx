import { Row } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components//ui/Flex";
import { Icon } from "components//ui/Icon";
import { Text } from "components//ui/Text";
import { Button } from "components/ui/Button";
import { Tooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./FieldCursorCell.module.scss";
import { isCursor } from "../../../syncCatalog/StreamFieldsTable/StreamFieldsTable";
import { updateCursorField } from "../../../syncCatalog/SyncCatalog/streamConfigHelpers";
import { checkCursorAndPKRequirements } from "../../../syncCatalog/utils";
import { SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

interface FieldCursorCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const FieldCursorCell: React.FC<FieldCursorCellProps> = ({ row, updateStreamField }) => {
  const { mode } = useConnectionFormService();
  const { streamNode, field } = row.original;

  if (!field || SyncSchemaFieldObject.isNestedField(field) || !streamNode.config?.selected) {
    return null;
  }

  const { cursorRequired, shouldDefineCursor } = checkCursorAndPKRequirements(streamNode.config, streamNode.stream!);

  // Cursor is not supported
  if (!cursorRequired) {
    return null;
  }

  const onCursorSelect = (cursorField: string[]) => {
    const numberOfFieldsInStream = Object.keys(streamNode.stream?.jsonSchema?.properties ?? {}).length ?? 0;
    const updatedConfig = updateCursorField(streamNode.config!, cursorField, numberOfFieldsInStream);
    updateStreamField(streamNode, updatedConfig);
  };

  const cursorDefined = shouldDefineCursor && SyncSchemaFieldObject.isPrimitive(field);
  const isSelectedCursor = isCursor(streamNode.config, field.path);
  const isDisabled = mode === "readonly";

  const cursorButton = isSelectedCursor ? (
    <Button
      type="button"
      variant="clear"
      className={styles.clearCursorButton}
      onClick={() => onCursorSelect(field?.path)}
      disabled={!cursorDefined || isDisabled}
    >
      <FlexContainer gap="sm" alignItems="center">
        <Icon type="cursor" color="primary" size="sm" />
        <Text color="blue">
          <FormattedMessage id="form.cursorField.label" />
        </Text>
      </FlexContainer>
    </Button>
  ) : (
    <Button
      type="button"
      variant="secondary"
      className={styles.cursorButton}
      disabled={!cursorDefined || isDisabled}
      onClick={() => onCursorSelect(field.path)}
    >
      <FormattedMessage id="form.cursorField.set" />
    </Button>
  );

  return isSelectedCursor && !cursorDefined ? (
    <Tooltip placement="bottom" control={cursorButton}>
      <FormattedMessage id="form.field.sourceDefinedCursor" />
      <TooltipLearnMoreLink url={links.sourceDefinedCursorLink} />
    </Tooltip>
  ) : (
    cursorButton
  );
};
