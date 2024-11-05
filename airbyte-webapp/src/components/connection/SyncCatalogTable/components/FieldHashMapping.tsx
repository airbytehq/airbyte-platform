import { Row } from "@tanstack/react-table";
import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { updateFieldHashing } from "components/connection/SyncCatalogTable/utils/streamConfigHelpers";
import { Option } from "components/ui/ListBox";
import { InlineListBox } from "components/ui/ListBox/InlineListBox";
import { Tooltip } from "components/ui/Tooltip";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { checkIsFieldHashed, checkIsFieldSelected, isCursor, isPrimaryKey } from "../utils/utils";

interface FieldHashMappingProps {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
}

type HashModeValue = "hashed" | "unhashed";

export const FieldHashMapping: React.FC<FieldHashMappingProps> = ({ row, updateStreamField }) => {
  const { formatMessage } = useIntl();
  const { mode } = useConnectionFormService();
  const isDisabled = mode === "readonly";
  const { field, streamNode } = row.original;

  const options: Array<Option<HashModeValue>> = useMemo(
    () => [
      { value: "unhashed", label: formatMessage({ id: "mapper.hash.optedOut" }) },
      { value: "hashed", label: formatMessage({ id: "mapper.hash.optedIn" }) },
    ],
    [formatMessage]
  );

  if (!field || !streamNode?.config || field.path.length > 1) {
    return null;
  }

  const isFieldSelected = checkIsFieldSelected(field, streamNode?.config);
  if (!isFieldSelected) {
    return null;
  }

  const isFieldHashed = checkIsFieldHashed(field, streamNode?.config);
  const isSelectedPrimaryKey = isPrimaryKey(streamNode.config, field.path);
  const isSelectedCursor = isCursor(streamNode.config, field.path);
  const isFieldHashingAllowed = !isSelectedPrimaryKey && !isSelectedCursor;

  const onChangeFieldHashing = (fieldPath: string[], isNowHashed: boolean) => {
    if (!row.original.streamNode) {
      return;
    }

    const updatedConfig = updateFieldHashing({
      config: streamNode.config!,
      fieldPath,
      isFieldHashed: isNowHashed,
    });

    updateStreamField(row.original.streamNode, updatedConfig);
  };

  const listbox = (
    <InlineListBox<HashModeValue>
      isDisabled={isDisabled || !isFieldHashingAllowed}
      options={options}
      selectedValue={isFieldHashed ? "hashed" : "unhashed"}
      onSelect={(value) => {
        onChangeFieldHashing(field.path, value === "hashed");
      }}
      placement="bottom-start"
      data-testid="field-hashing-listbox"
    />
  );

  return (
    <div data-showonhover={!isFieldHashed}>
      <Tooltip placement="top" disabled={isFieldHashingAllowed} control={listbox}>
        <FormattedMessage
          id={isSelectedPrimaryKey ? "connectionForm.hashing.pk.tip" : "connectionForm.hashing.cursor.tip"}
        />
      </Tooltip>
    </div>
  );
};
