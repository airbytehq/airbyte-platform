import { ColumnFilter, Row } from "@tanstack/react-table";
import React, { useMemo } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import { DestinationNamespaceFormValues, DestinationNamespaceModal } from "../../../DestinationNamespaceModal";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";
import { getColumnFilterValue } from "../utils";

interface NamespaceNameCellProps extends Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat"> {
  row: Row<SyncCatalogUIModel>;
  streams: SyncStreamFieldWithId[];
  onStreamsChanged: (streams: SyncStreamFieldWithId[]) => void;
  syncCheckboxDisabled?: boolean;
  columnFilters: ColumnFilter[];
}

export const NamespaceNameCell: React.FC<NamespaceNameCellProps> = ({
  row,
  streams,
  onStreamsChanged,
  syncCheckboxDisabled,
  namespaceDefinition,
  namespaceFormat,
  columnFilters,
}) => {
  const { mode } = useConnectionFormService();
  const { name: namespaceName } = row.original;
  const { openModal } = useModalService();
  const { setValue } = useFormContext<FormConnectionFormValues>();

  const [allEnabled, partiallyEnabled, countedStreams, totalCount, showTotalCount] = useMemo(() => {
    const subRows = row.original.subRows || [];
    const enabledCount = subRows.filter(({ streamNode }) => streamNode?.config?.selected).length;
    const totalCount = subRows.length;
    const allEnabled = totalCount === enabledCount;
    const columnFilterValue = getColumnFilterValue(columnFilters, "stream.selected");
    const counted =
      columnFilterValue === undefined ? totalCount : columnFilterValue ? enabledCount : totalCount - enabledCount;
    const partiallyEnabled = enabledCount > 0 && enabledCount < totalCount;
    const showTotalCount = columnFilterValue === undefined || totalCount === counted;

    return [allEnabled, partiallyEnabled, counted, totalCount, showTotalCount];
  }, [row.original.subRows, columnFilters]);

  const onToggleAllStreamsInNamespace = ({ target: { checked } }: React.ChangeEvent<HTMLInputElement>) => {
    const updateStream = (stream: SyncStreamFieldWithId) =>
      ({
        ...stream,
        config: { ...stream.config, selected: checked },
      }) as SyncStreamFieldWithId;

    // if we have the only one namespace
    if (totalCount === streams.length) {
      onStreamsChanged(streams.map(updateStream));
      return;
    }

    // if we have multiple namespaces
    const namespaceStreamFieldIds = row.original.subRows?.map(({ streamNode }) => streamNode?.id);
    onStreamsChanged(
      streams.map((stream) => (namespaceStreamFieldIds?.includes(stream.id) ? updateStream(stream) : stream))
    );
  };

  const destinationNamespaceChange = (value: DestinationNamespaceFormValues) => {
    setValue("namespaceDefinition", value.namespaceDefinition, { shouldDirty: true });

    if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
      setValue("namespaceFormat", value.namespaceFormat);
    }
  };

  return (
    <FlexContainer alignItems="center">
      <CheckBox
        checkboxSize="sm"
        indeterminate={partiallyEnabled}
        checked={allEnabled}
        onChange={onToggleAllStreamsInNamespace}
        disabled={syncCheckboxDisabled || mode === "readonly"}
        data-testid="sync-namespace-checkbox"
      />
      {namespaceName && (
        <Text
          bold={!!namespaceName}
          size="sm"
          color={namespaceName ? "darkBlue" : "grey400"}
          italicized={!namespaceName}
        >
          {namespaceName}
        </Text>
      )}
      <FlexContainer gap="none" alignItems="center">
        <Text size="sm" color="grey300">
          <FormattedMessage
            id={showTotalCount ? "form.amountOfStreams" : "form.amountOfCountedStreamsOutOfTotal"}
            values={showTotalCount ? { count: totalCount } : { countedStreams, count: totalCount }}
          />
        </Text>
        <Button
          type="button"
          variant="clear"
          disabled={mode === "readonly"}
          onClick={() =>
            openModal<void>({
              size: "lg",
              title: <FormattedMessage id="connectionForm.modal.destinationNamespace.title" />,
              content: ({ onComplete, onCancel }) => (
                <DestinationNamespaceModal
                  initialValues={{
                    namespaceDefinition,
                    namespaceFormat,
                  }}
                  onCancel={onCancel}
                  onSubmit={async (values) => {
                    destinationNamespaceChange(values);
                    onComplete();
                  }}
                />
              ),
            })
          }
        >
          <Icon type="gear" size="sm" />
        </Button>
      </FlexContainer>
    </FlexContainer>
  );
};
