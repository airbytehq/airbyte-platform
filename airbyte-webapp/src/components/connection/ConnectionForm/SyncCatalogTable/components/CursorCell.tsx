import { Row } from "@tanstack/react-table";
import get from "lodash/get";
import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import ValidationError from "yup/lib/ValidationError";

import { Option } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";

import { CatalogComboBox } from "./CatalogComboBox/CatalogComboBox";
import styles from "./NextCursorCell.module.scss";
import { updateCursorField } from "../../../syncCatalog/SyncCatalog/streamConfigHelpers";
import { checkCursorAndPKRequirements, getFieldPathType } from "../../../syncCatalog/utils";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { pathDisplayName } from "../utils";

interface NextCursorCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const CursorCell: React.FC<NextCursorCellProps> = ({ row, updateStreamField }) => {
  const { config, stream } = row.original.streamNode;
  const { errors } = useFormState<FormConnectionFormValues>();

  const { cursorRequired, shouldDefineCursor } = checkCursorAndPKRequirements(config!, stream!);
  const cursorType = getFieldPathType(cursorRequired, shouldDefineCursor);

  const cursorOptions: Option[] =
    row.original.subRows
      ?.filter((subRow) => subRow?.field && !SyncSchemaFieldObject.isNestedField(subRow?.field))
      .map((subRow) => subRow?.field?.cleanedName ?? "")
      .sort()
      .map((name) => ({ value: name })) ?? [];

  const cursorValue =
    cursorType === "sourceDefined"
      ? pathDisplayName(stream?.defaultCursorField ?? [])
      : cursorType === "required"
      ? pathDisplayName(config?.cursorField ?? [])
      : "";

  const cursorConfigValidationError: ValidationError | undefined = get(
    errors,
    `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config.cursorField`
  );

  const onChange = (cursor: string) => {
    const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;
    const updatedConfig = updateCursorField(config!, [cursor], numberOfFieldsInStream);
    updateStreamField(row.original.streamNode, updatedConfig);
  };

  return config?.selected && cursorType ? (
    <FlexContainer direction="row" gap="xs" alignItems="center" data-testid="cursor-field-cell">
      <CatalogComboBox
        disabled={!shouldDefineCursor}
        options={cursorOptions}
        value={cursorValue}
        onChange={onChange}
        error={cursorConfigValidationError}
        // in case we have a source defined cursor, but the value is not in the list of options show a placeholder
        buttonPlaceholder={
          cursorType === "sourceDefined" && (
            <Text italicized size="sm">
              <FormattedMessage id="connection.catalogTree.sourceDefined" />
            </Text>
          )
        }
        buttonErrorText={<FormattedMessage id="form.error.cursor.missing" />}
        buttonAddText={<FormattedMessage id="form.error.cursor.addMissing" />}
        buttonEditText={<FormattedMessage id="form.error.cursor.edit" />}
        controlClassName={styles.control}
        controlBtnIcon="cursor"
      />
    </FlexContainer>
  ) : null;
};
