import { faGear } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { setIn, useFormikContext } from "formik";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FormikConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { InfoTooltip, TooltipLearnMoreLink } from "components/ui/Tooltip";

import { SyncSchemaStream } from "core/domain/catalog";
import { NamespaceDefinitionType } from "core/request/AirbyteClient";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";

import styles from "./StreamsConfigTableHeader.module.scss";
import {
  DestinationNamespaceHookFormModal,
  DestinationNamespaceHookFormValueType,
} from "../../DestinationNamespaceModal/DestinationNamespaceHookFormModal";
import {
  DestinationNamespaceFormValueType,
  DestinationNamespaceModal,
} from "../../DestinationNamespaceModal/DestinationNamespaceModal";
import {
  DestinationStreamNamesHookFormModal,
  DestinationStreamNamesHookFormValueType,
  StreamNameDefinitionValueHookFormType,
} from "../../DestinationStreamNamesModal/DestinationStreamNamesHookFormModal";
import {
  DestinationStreamNamesFormValueType,
  DestinationStreamNamesModal,
  StreamNameDefinitionValueType,
} from "../../DestinationStreamNamesModal/DestinationStreamNamesModal";
import { CellText, CellTextProps } from "../CellText";

const HeaderCell: React.FC<React.PropsWithChildren<CellTextProps>> = ({ children, ...tableCellProps }) => (
  <CellText {...tableCellProps} withOverflow>
    <Text size="sm" color="grey300">
      {children}
    </Text>
  </CellText>
);

interface StreamsConfigTableHeaderProps {
  streams: SyncSchemaStream[];
  onStreamsChanged: (streams: SyncSchemaStream[]) => void;
  syncSwitchDisabled?: boolean;
}

export const StreamsConfigTableHeader: React.FC<StreamsConfigTableHeaderProps> = ({
  streams,
  onStreamsChanged,
  syncSwitchDisabled,
}) => {
  const { mode } = useConnectionFormService();
  const { openModal, closeModal } = useModalService();
  const doUseReactHookForm = useExperiment("form.reactHookForm", false);
  const formikProps = useFormikContext<FormikConnectionFormValues>();

  const destinationNamespaceHookFormChange = (value: DestinationNamespaceHookFormValueType) => {
    formikProps.setFieldValue("namespaceDefinition", value.namespaceDefinition);

    if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
      formikProps.setFieldValue("namespaceFormat", value.namespaceFormat);
    }
  };

  // TODO: remove after DestinationNamespaceModal migration
  const destinationNamespaceChange = (value: DestinationNamespaceFormValueType) => {
    formikProps.setFieldValue("namespaceDefinition", value.namespaceDefinition);

    if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
      formikProps.setFieldValue("namespaceFormat", value.namespaceFormat);
    }
  };

  const destinationStreamNameHookFormChange = (value: DestinationStreamNamesHookFormValueType) => {
    formikProps.setFieldValue(
      "prefix",
      value.streamNameDefinition === StreamNameDefinitionValueHookFormType.Prefix ? value.prefix : ""
    );
  };

  // TODO: remove after DestinationStreamNamesModal migration
  const destinationStreamNamesChange = (value: DestinationStreamNamesFormValueType) => {
    formikProps.setFieldValue(
      "prefix",
      value.streamNameDefinition === StreamNameDefinitionValueType.Prefix ? value.prefix : ""
    );
  };

  const onToggleAllStreamsSyncSwitch = ({ target: { checked } }: React.ChangeEvent<HTMLInputElement>) =>
    onStreamsChanged(
      streams.map((stream) =>
        setIn(stream, "config", {
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
              content: () =>
                doUseReactHookForm ? (
                  <DestinationNamespaceHookFormModal
                    initialValues={{
                      namespaceDefinition: formikProps.values.namespaceDefinition,
                      namespaceFormat: formikProps.values.namespaceFormat,
                    }}
                    onCloseModal={closeModal}
                    onSubmit={destinationNamespaceHookFormChange}
                  />
                ) : (
                  <DestinationNamespaceModal
                    initialValues={{
                      namespaceDefinition: formikProps.values.namespaceDefinition,
                      namespaceFormat: formikProps.values.namespaceFormat,
                    }}
                    onCloseModal={closeModal}
                    onSubmit={destinationNamespaceChange}
                  />
                ),
            })
          }
        >
          <FontAwesomeIcon icon={faGear} />
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
              content: () =>
                doUseReactHookForm ? (
                  <DestinationStreamNamesHookFormModal
                    initialValues={{
                      prefix: formikProps.values.prefix,
                    }}
                    onCloseModal={closeModal}
                    onSubmit={destinationStreamNameHookFormChange}
                  />
                ) : (
                  <DestinationStreamNamesModal
                    initialValues={{
                      prefix: formikProps.values.prefix,
                    }}
                    onCloseModal={closeModal}
                    onSubmit={destinationStreamNamesChange}
                  />
                ),
            })
          }
        >
          <FontAwesomeIcon icon={faGear} />
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
