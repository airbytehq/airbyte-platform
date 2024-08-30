import set from "lodash/set";
import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { AirbyteStreamAndConfiguration, NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import { DestinationNamespaceFormValues, DestinationNamespaceModal } from "../../../DestinationNamespaceModal";
import { FormConnectionFormValues } from "../../formConfig";

interface HeaderNamespaceCellProps extends Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat"> {
  streams: AirbyteStreamAndConfiguration[];
  onStreamsChanged: (streams: AirbyteStreamAndConfiguration[]) => void;
  syncCheckboxDisabled?: boolean;
}

export const HeaderNamespaceCell: React.FC<HeaderNamespaceCellProps> = ({
  streams,
  onStreamsChanged,
  syncCheckboxDisabled,
  namespaceDefinition,
  namespaceFormat,
}) => {
  const { mode } = useConnectionFormService();
  const { openModal } = useModalService();
  const { setValue } = useFormContext<FormConnectionFormValues>();

  const destinationNamespaceChange = (value: DestinationNamespaceFormValues) => {
    setValue("namespaceDefinition", value.namespaceDefinition, { shouldDirty: true });

    if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
      setValue("namespaceFormat", value.namespaceFormat);
    }
  };

  const onToggleAllStreamsSyncSwitch = ({ target: { checked } }: React.ChangeEvent<HTMLInputElement>) =>
    onStreamsChanged(
      streams.map((stream) =>
        set(stream, "config", {
          ...stream.config,
          selected: checked,
        })
      )
    );
  const isPartOfStreamsSyncEnabled = () =>
    streams.some((stream) => stream.config?.selected) &&
    streams.filter((stream) => stream.config?.selected).length !== streams.length;
  const areAllStreamsSyncEnabled = () => streams.every((stream) => stream.config?.selected) && streams.length > 0;

  return (
    <FlexContainer alignItems="center">
      <CheckBox
        checkboxSize="sm"
        indeterminate={isPartOfStreamsSyncEnabled()}
        checked={areAllStreamsSyncEnabled()}
        onChange={onToggleAllStreamsSyncSwitch}
        disabled={syncCheckboxDisabled || !streams.length || mode === "readonly"}
      />
      <FlexContainer gap="none" alignItems="center">
        <Text size="sm" color="grey300">
          <FormattedMessage id="form.amountOfStreams" values={{ count: streams.length }} />
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
