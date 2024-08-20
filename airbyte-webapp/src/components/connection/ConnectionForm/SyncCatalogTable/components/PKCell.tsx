import { Row } from "@tanstack/react-table";
import get from "lodash/get";
import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import ValidationError from "yup/lib/ValidationError";

import { Option } from "components/ui/ComboBox";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaFieldObject } from "core/domain/catalog";

import { MultiCatalogComboBox } from "./CatalogComboBox/CatalogComboBox";
import styles from "./NextPKCell.module.scss";
import { updatePrimaryKey } from "../../../syncCatalog/SyncCatalog/streamConfigHelpers";
import { checkCursorAndPKRequirements, getFieldPathType } from "../../../syncCatalog/utils";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { pathDisplayName } from "../utils";

interface NextPKCellProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

export const PKCell: React.FC<NextPKCellProps> = ({ row, updateStreamField }) => {
  const { config, stream } = row.original.streamNode;
  const { errors } = useFormState<FormConnectionFormValues>();

  const { pkRequired, shouldDefinePk } = checkCursorAndPKRequirements(config!, stream!);
  const pkType = getFieldPathType(pkRequired, shouldDefinePk);

  const pkOptions: Option[] =
    row.original.subRows
      ?.filter((subRow) => subRow?.field && !SyncSchemaFieldObject.isNestedField(subRow?.field))
      .map((subRow) => subRow?.field?.cleanedName ?? "")
      .sort()
      .map((name) => ({ value: name })) ?? [];

  const pkConfigValidationError: ValidationError | undefined = get(
    errors,
    `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config.primaryKey`
  );

  const onChange = (selectedPKs: string[]) => {
    const numberOfFieldsInStream = Object.keys(stream?.jsonSchema?.properties ?? {}).length ?? 0;

    const updatedConfig = updatePrimaryKey(
      config!,
      selectedPKs.map((pk) => [pk]),
      numberOfFieldsInStream
    );
    updateStreamField(row.original.streamNode, updatedConfig);
  };

  return config?.selected && pkType ? (
    <div data-testid="primary-key-cell">
      <MultiCatalogComboBox
        options={pkOptions}
        disabled={!shouldDefinePk}
        value={config?.primaryKey?.map(pathDisplayName)}
        onChange={onChange}
        maxSelectedLabels={1}
        error={pkConfigValidationError}
        buttonErrorText={<FormattedMessage id="form.error.pk.missing" />}
        buttonAddText={<FormattedMessage id="form.error.pk.addMissing" />}
        buttonEditText={<FormattedMessage id="form.error.pk.edit" />}
        controlClassName={styles.control}
        controlBtnIcon="keyCircle"
      />
    </div>
  ) : null;
};
