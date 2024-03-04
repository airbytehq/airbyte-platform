import set from "lodash/set";
import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { InfoTooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { AirbyteStreamAndConfiguration, NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import styles from "./StreamsConfigTableHeader.module.scss";
import { FormConnectionFormValues } from "../../ConnectionForm/formConfig";
import { DestinationNamespaceModal, DestinationNamespaceFormValues } from "../../DestinationNamespaceModal";
import {
  DestinationStreamNamesModal,
  DestinationStreamNamesFormValues,
  StreamNameDefinitionValueType,
} from "../../DestinationStreamNamesModal";
import { CellText, CellTextProps } from "../CellText";

const HeaderCell: React.FC<React.PropsWithChildren<CellTextProps>> = ({ children, ...tableCellProps }) => (
  <CellText {...tableCellProps} withOverflow>
    <Text size="sm" color="grey300">
      {children}
    </Text>
  </CellText>
);

interface StreamsConfigTableHeaderProps
  extends Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat" | "prefix"> {
  streams: AirbyteStreamAndConfiguration[];
  onStreamsChanged: (streams: AirbyteStreamAndConfiguration[]) => void;
  syncSwitchDisabled?: boolean;
}

export const StreamsConfigTableHeader: React.FC<StreamsConfigTableHeaderProps> = ({
  streams,
  onStreamsChanged,
  syncSwitchDisabled,
  namespaceDefinition,
  namespaceFormat,
  prefix,
}) => {
  const { mode } = useConnectionFormService();
  const { openModal, closeModal } = useModalService();
  const { setValue } = useFormContext<FormConnectionFormValues>();

  const destinationNamespaceChange = (value: DestinationNamespaceFormValues) => {
    setValue("namespaceDefinition", value.namespaceDefinition, { shouldDirty: true });

    if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
      setValue("namespaceFormat", value.namespaceFormat);
    }
  };

  const destinationStreamNameChange = (value: DestinationStreamNamesFormValues) => {
    setValue("prefix", value.streamNameDefinition === StreamNameDefinitionValueType.Prefix ? value.prefix : "", {
      shouldDirty: true,
    });
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
    <FlexContainer
      justifyContent="flex-start"
      alignItems="center"
      className={styles.headerContainer}
      data-testid="catalog-tree-table-header"
    >
      <CellText size="fixed" className={styles.syncCell} withOverflow>
        <Switch
          size="sm"
          indeterminate={isPartOfStreamsSyncEnabled()}
          checked={areAllStreamsSyncEnabled()}
          onChange={onToggleAllStreamsSyncSwitch}
          disabled={syncSwitchDisabled || !streams.length || mode === "readonly"}
          id="all-streams-sync-switch"
          data-testid="all-streams-sync-switch"
        />
        <Text size="sm" color="grey300">
          <FormattedMessage id="sources.sync" />
        </Text>
      </CellText>
      <HeaderCell size="fixed" className={styles.dataDestinationCell}>
        <FormattedMessage id="form.dataDestination" />
        <Button
          type="button"
          variant="clear"
          disabled={mode === "readonly"}
          onClick={() =>
            openModal({
              size: "lg",
              title: <FormattedMessage id="connectionForm.modal.destinationNamespace.title" />,
              content: () => (
                <DestinationNamespaceModal
                  initialValues={{
                    namespaceDefinition,
                    namespaceFormat,
                  }}
                  onCloseModal={closeModal}
                  onSubmit={destinationNamespaceChange}
                />
              ),
            })
          }
        >
          <Icon type="gear" size="sm" />
        </Button>
      </HeaderCell>
      <HeaderCell>
        <FormattedMessage id="form.stream" />
        <Button
          type="button"
          variant="clear"
          disabled={mode === "readonly"}
          onClick={() =>
            openModal({
              size: "sm",
              title: <FormattedMessage id="connectionForm.modal.destinationStreamNames.title" />,
              content: () => (
                <DestinationStreamNamesModal
                  initialValues={{
                    prefix,
                  }}
                  onCloseModal={closeModal}
                  onSubmit={destinationStreamNameChange}
                />
              ),
            })
          }
        >
          <Icon type="gear" size="sm" />
        </Button>
      </HeaderCell>
      <HeaderCell className={styles.syncModeCell}>
        <FormattedMessage id="form.syncMode" />
        <InfoTooltip>
          <FormattedMessage id="connectionForm.syncType.info" />
          <TooltipLearnMoreLink url={links.syncModeLink} />
        </InfoTooltip>
      </HeaderCell>
      <HeaderCell />
      <HeaderCell size="fixed" className={styles.fieldsCell}>
        <FormattedMessage id="form.fields" />
      </HeaderCell>
    </FlexContainer>
  );
};
