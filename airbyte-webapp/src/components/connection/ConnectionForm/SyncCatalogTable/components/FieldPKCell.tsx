import { Row } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./FieldPKCell.module.scss";
import { isPrimaryKey } from "../../../syncCatalog/StreamFieldsTable/StreamFieldsTable";
import { toggleFieldInPrimaryKey } from "../../../syncCatalog/SyncCatalog/streamConfigHelpers";
import { checkCursorAndPKRequirements } from "../../../syncCatalog/utils";
import { SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

export interface FieldPKCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const FieldPKCell: React.FC<FieldPKCellProps> = ({ row, updateStreamField }) => {
  const { mode } = useConnectionFormService();
  const { streamNode, field } = row.original;

  if (!streamNode || !field) {
    return null;
  }

  const { pkRequired, shouldDefinePk } = checkCursorAndPKRequirements(streamNode.config!, streamNode.stream!);

  // PK is not supported
  if (!pkRequired) {
    return null;
  }

  const onPkSelect = (pkPath: string[]) => {
    const numberOfFieldsInStream = Object.keys(streamNode.stream?.jsonSchema?.properties ?? {}).length ?? 0;
    const updatedConfig = toggleFieldInPrimaryKey(streamNode.config!, pkPath, numberOfFieldsInStream);
    updateStreamField(streamNode, updatedConfig);
  };

  const primaryKeyDefined = shouldDefinePk && SyncSchemaFieldObject.isPrimitive(field);
  const isSelectedPrimaryKey = isPrimaryKey(streamNode.config, field.path);
  const isDisabled = mode === "readonly";

  const pkButton = isSelectedPrimaryKey ? (
    <Button
      type="button"
      variant="clear"
      className={styles.clearPkButton}
      onClick={() => onPkSelect(field.path)}
      disabled={!primaryKeyDefined || isDisabled}
    >
      <FlexContainer gap="sm" alignItems="center">
        <Icon type="keyCircle" color="primary" size="sm" />
        <Text color="blue">
          <FormattedMessage id="form.primaryKey.label" />
        </Text>
      </FlexContainer>
    </Button>
  ) : (
    <Button
      type="button"
      variant="secondary"
      className={styles.pkButton}
      disabled={!primaryKeyDefined || isDisabled}
      onClick={() => onPkSelect(field.path)}
    >
      <FormattedMessage id="form.primaryKey.set" />
    </Button>
  );

  return isSelectedPrimaryKey && !primaryKeyDefined ? (
    <Tooltip placement="bottom" control={pkButton}>
      <FormattedMessage id="form.field.sourceDefinedPK" />
      <TooltipLearnMoreLink url={links.sourceDefinedPKLink} />
    </Tooltip>
  ) : (
    pkButton
  );
};
