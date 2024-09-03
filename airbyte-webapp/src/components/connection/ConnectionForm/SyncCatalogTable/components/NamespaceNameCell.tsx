import { Row } from "@tanstack/react-table";
import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { AirbyteStreamConfiguration, NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import { DestinationNamespaceFormValues, DestinationNamespaceModal } from "../../../DestinationNamespaceModal";
import { FormConnectionFormValues, SyncStreamFieldWithId } from "../../formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

interface NamespaceNameCellProps extends Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat"> {
  row: Row<SyncCatalogUIModel>;
  updateStreamField: (streamNode: SyncStreamFieldWithId, updatedConfig: Partial<AirbyteStreamConfiguration>) => void;
  syncCheckboxDisabled?: boolean;
}

export const NamespaceNameCell: React.FC<NamespaceNameCellProps> = ({
  row,
  updateStreamField,
  syncCheckboxDisabled,
  namespaceDefinition,
  namespaceFormat,
}) => {
  const { mode } = useConnectionFormService();
  const { name: namespaceName } = row.original;
  const { openModal } = useModalService();
  const { setValue } = useFormContext<FormConnectionFormValues>();

  const destinationNamespaceChange = (value: DestinationNamespaceFormValues) => {
    setValue("namespaceDefinition", value.namespaceDefinition, { shouldDirty: true });

    if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
      setValue("namespaceFormat", value.namespaceFormat);
    }
  };

  const onToggleAllStreamsSyncSwitch = ({ target: { checked } }: React.ChangeEvent<HTMLInputElement>) => {
    if (!row.original.subRows) {
      return;
    }
    row.original.subRows.forEach(({ streamNode }) => {
      updateStreamField(streamNode!, { selected: checked });
    });
  };

  const isPartOfStreamsSyncEnabled = () => {
    if (!row.original.subRows) {
      return;
    }

    return (
      row.original.subRows.some(({ streamNode }) => streamNode?.config?.selected) &&
      row.original.subRows.filter(({ streamNode }) => streamNode?.config?.selected).length !==
        row.original.subRows.length
    );
  };
  const areAllStreamsSyncEnabled = () => row.original.subRows?.every(({ streamNode }) => streamNode?.config?.selected);

  return (
    <FlexContainer alignItems="center">
      <CheckBox
        checkboxSize="sm"
        indeterminate={isPartOfStreamsSyncEnabled()}
        checked={areAllStreamsSyncEnabled()}
        onChange={onToggleAllStreamsSyncSwitch}
        disabled={syncCheckboxDisabled || mode === "readonly"}
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
          <FormattedMessage id="form.amountOfStreams" values={{ count: row.original.subRows?.length }} />
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
