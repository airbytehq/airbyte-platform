import { Row } from "@tanstack/react-table";
import get from "lodash/get";
import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import ValidationError from "yup/lib/ValidationError";

import { Option } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { MultiCatalogComboBox } from "./CatalogComboBox/CatalogComboBox";
import styles from "./StreamPKCell.module.scss";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import {
  checkIsFieldHashed,
  getFieldPathType,
  checkCursorAndPKRequirements,
  getFieldPathDisplayName,
  updatePrimaryKey,
} from "../utils";

interface NextPKCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const StreamPKCell: React.FC<NextPKCellProps> = ({ row, updateStreamField }) => {
  const { mode } = useConnectionFormService();
  const { errors } = useFormState<FormConnectionFormValues>();

  if (!row.original.streamNode || !row.original.streamNode.config || !row.original.streamNode.stream) {
    return null;
  }
  const { config, stream } = row.original.streamNode;
  const { pkRequired, shouldDefinePk } = checkCursorAndPKRequirements(config, stream);
  const pkType = getFieldPathType(pkRequired, shouldDefinePk);

  const pkOptions: Option[] =
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

  const pkConfigValidationError: ValidationError | undefined = get(
    errors,
    `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config.primaryKey`
  );

  const onChange = (selectedPKs: string[]) => {
    const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;

    const updatedConfig = updatePrimaryKey(
      config,
      selectedPKs.map((pk) => [pk]),
      numberOfFieldsInStream
    );
    if (row.original.streamNode) {
      updateStreamField(row.original.streamNode, updatedConfig);
    }
  };

  const isSourceDefined = config?.selected && !shouldDefinePk;
  const isMultiplePKs = config?.selected && config?.primaryKey && config?.primaryKey?.length > 1;

  const tooltipContent =
    isSourceDefined || isMultiplePKs ? (
      <FlexContainer gap="lg" direction="column">
        {isSourceDefined && (
          <div>
            <FormattedMessage id="form.field.sourceDefinedPK" />
            <TooltipLearnMoreLink url={links.sourceDefinedPKLink} />
          </div>
        )}
        {isMultiplePKs && (
          <ul className={styles.pkList}>
            {config?.primaryKey
              ?.map((pk) => getFieldPathDisplayName(pk))
              .sort((a, b) => a.localeCompare(b))
              .map((pk) => <li key={pk}>{pk}</li>)}
          </ul>
        )}
      </FlexContainer>
    ) : null;

  return (
    <div data-testid="primary-key-cell">
      {config?.selected && pkType ? (
        <MultiCatalogComboBox
          options={pkOptions}
          disabled={!shouldDefinePk || mode === "readonly"}
          value={config?.primaryKey?.map(getFieldPathDisplayName)}
          onChange={onChange}
          maxSelectedLabels={1}
          error={pkConfigValidationError}
          buttonErrorText={<FormattedMessage id="form.error.pk.missing" />}
          buttonAddText={<FormattedMessage id="form.error.pk.addMissing" />}
          buttonEditText={<FormattedMessage id="form.error.pk.edit" />}
          controlClassName={styles.control}
          controlBtnIcon="keyCircle"
          controlBtnTooltipContent={tooltipContent}
        />
      ) : null}
    </div>
  );
};
