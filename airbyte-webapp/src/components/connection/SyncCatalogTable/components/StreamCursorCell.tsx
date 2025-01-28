import { Row } from "@tanstack/react-table";
import get from "lodash/get";
import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import ValidationError from "yup/lib/ValidationError";

import { Option } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { CatalogComboBox } from "./CatalogComboBox/CatalogComboBox";
import styles from "./StreamCursorCell.module.scss";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { checkIsFieldHashed, getFieldPathType, checkCursorAndPKRequirements, getFieldPathDisplayName } from "../utils";
import { updateCursorField } from "../utils/streamConfigHelpers";

interface NextCursorCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const StreamCursorCell: React.FC<NextCursorCellProps> = ({ row, updateStreamField }) => {
  const { mode } = useConnectionFormService();
  const { errors } = useFormState<FormConnectionFormValues>();

  if (!row.original?.streamNode || !row.original.streamNode.config || !row.original.streamNode.stream) {
    return null;
  }

  const { config, stream } = row.original.streamNode;

  const { cursorRequired, shouldDefineCursor } = checkCursorAndPKRequirements(config, stream);
  const cursorType = getFieldPathType(cursorRequired, shouldDefineCursor);

  const cursorOptions: Option[] =
    row.original.subRows
      ?.filter((subRow) => subRow?.field && !SyncSchemaFieldObject.isNestedField(subRow?.field))
      .map<SyncCatalogUIModel & { disabled?: boolean; disabledReason?: React.ReactNode }>((subRow) => {
        const { field, streamNode } = subRow;
        // typescript validation
        if (!field || !streamNode?.config || field.path.length > 1) {
          return subRow;
        }
        return checkIsFieldHashed(field, streamNode.config) ? { ...subRow, disabled: true } : subRow;
      })
      .map((subRow) => ({ subRow, cleanedName: subRow?.field?.cleanedName ?? "" }))
      .sort((a, b) => {
        return a.cleanedName.localeCompare(b.cleanedName);
      })
      .map(({ cleanedName, subRow }) => ({
        value: cleanedName,
        disabled: subRow.disabled,
        disabledReason: <FormattedMessage id="connectionForm.hashing.preventing.tip" />,
      })) ?? [];

  const cursorValue =
    cursorType === "sourceDefined"
      ? getFieldPathDisplayName(stream?.defaultCursorField ?? [])
      : cursorType === "required"
      ? getFieldPathDisplayName(config?.cursorField ?? [])
      : "";

  const cursorConfigValidationError: ValidationError | undefined = get(
    errors,
    `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config.cursorField`
  );

  const onChange = (cursor: string) => {
    const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;
    const updatedConfig = updateCursorField(config, [cursor], numberOfFieldsInStream);
    if (row.original?.streamNode) {
      updateStreamField(row.original.streamNode, updatedConfig);
    }
  };

  const tooltipContent = !shouldDefineCursor ? (
    <FlexContainer gap="lg" direction="column">
      <FormattedMessage id="form.field.sourceDefinedCursor" />
      <TooltipLearnMoreLink url={links.sourceDefinedCursorLink} />
    </FlexContainer>
  ) : null;

  return (
    <div data-testid="cursor-field-cell">
      {config?.selected && cursorType ? (
        <CatalogComboBox
          disabled={!shouldDefineCursor || mode === "readonly"}
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
          controlBtnTooltipContent={tooltipContent}
        />
      ) : null}
    </div>
  );
};
