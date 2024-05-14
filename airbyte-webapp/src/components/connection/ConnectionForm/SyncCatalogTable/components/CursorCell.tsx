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

interface CursorCellProps {
  row: Row<SyncCatalogUIModel>;
}

export const CursorCell: React.FC<CursorCellProps> = ({ row }) => {
  const { config, stream } = row.original.streamNode;
  const { errors } = useFormState<FormConnectionFormValues>();

  const { cursorRequired, shouldDefineCursor } = checkCursorAndPKRequirements(config!, stream!);
  const cursorType = getFieldPathType(cursorRequired, shouldDefineCursor);

  const cursorFieldString = useMemo(() => {
    if (cursorType === "sourceDefined") {
      if (stream?.defaultCursorField?.length) {
        return pathDisplayName(stream.defaultCursorField);
      }
      return <FormattedMessage id="connection.catalogTree.sourceDefined" />;
    } else if (cursorType === "required" && config?.cursorField?.length) {
      return pathDisplayName(config?.cursorField);
    }
    return <FormattedMessage id="form.error.cursor.missing" />;
  }, [config?.cursorField, cursorType, stream?.defaultCursorField]);

  const cursorConfigValidationError: ValidationError | undefined = get(
    errors,
    `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config.cursorField`
  );

  return config?.selected && cursorType ? (
    <FlexContainer direction="row" gap="xs" alignItems="center" data-testid="cursor-field-cell">
      <Icon type="cursor" size="sm" color={cursorConfigValidationError ? "error" : "action"} />
      <TextWithOverflowTooltip color={cursorConfigValidationError ? "red" : "grey"}>
        {cursorFieldString}
      </TextWithOverflowTooltip>
    </FlexContainer>
  ) : null;
};
