import { Row } from "@tanstack/react-table";
import get from "lodash/get";
import React, { useMemo } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import ValidationError from "yup/lib/ValidationError";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { TextWithOverflowTooltip } from "components/ui/Text";

import { checkCursorAndPKRequirements, getFieldPathType } from "../../../syncCatalog/utils";
import { FormConnectionFormValues } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { pathDisplayName } from "../utils";

interface PKCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const PKCell: React.FC<PKCellProps> = ({ row }) => {
  const { config, stream } = row.original.streamNode;
  const { errors } = useFormState<FormConnectionFormValues>();

  const { pkRequired, shouldDefinePk } = checkCursorAndPKRequirements(config!, stream!);
  const pkType = getFieldPathType(pkRequired, shouldDefinePk);

  const primaryKeyString = useMemo(() => {
    if (!config) {
      return;
    }
    if (!config.primaryKey?.length) {
      return <FormattedMessage id="form.error.pk.missing" />;
    }
    return config.primaryKey.map(pathDisplayName).join(", ");
  }, [config]);

  const pkConfigValidationError: ValidationError | undefined = get(
    errors,
    `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config.primaryKey`
  );

  return config?.selected && pkType ? (
    <FlexContainer direction="row" gap="xs" alignItems="center" data-testid="primary-key-cell">
      <Icon type="keyCircle" size="sm" color={pkConfigValidationError ? "error" : "action"} />
      <TextWithOverflowTooltip color={pkConfigValidationError ? "red" : "grey"}>
        {primaryKeyString}
      </TextWithOverflowTooltip>
    </FlexContainer>
  ) : null;
};
