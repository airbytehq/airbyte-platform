import { useFormikContext } from "formik";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionScheduleType, NamespaceDefinitionType } from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionConfigurationFormPreview.module.scss";
import { FormikConnectionFormValues } from "./formConfig";
import { namespaceDefinitionOptions } from "./types";

const Frequency: React.FC = () => {
  const {
    values: { scheduleType, scheduleData },
  } = useFormikContext<FormikConnectionFormValues>();

  return (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id="form.frequency" />:
      </Text>
      <Text size="md" color="grey">
        {scheduleType === ConnectionScheduleType.manual && <FormattedMessage id="frequency.manual" />}
        {scheduleType === ConnectionScheduleType.cron && (
          <>
            <FormattedMessage id="frequency.cron" /> - {scheduleData?.cron?.cronExpression}{" "}
            {scheduleData?.cron?.cronTimeZone}
          </>
        )}
        {scheduleType === ConnectionScheduleType.basic && (
          <FormattedMessage
            id={`form.every.${scheduleData?.basicSchedule?.timeUnit}`}
            values={{ value: scheduleData?.basicSchedule?.units }}
          />
        )}
      </Text>
    </div>
  );
};

const DestinationNamespace: React.FC = () => {
  const {
    values: { namespaceDefinition, namespaceFormat },
  } = useFormikContext<FormikConnectionFormValues>();

  return (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id="connectionForm.namespaceDefinition.title" />:
      </Text>
      <Text size="md" color="grey">
        <FormattedMessage
          id={`connectionForm.${namespaceDefinitionOptions[namespaceDefinition as NamespaceDefinitionType]}`}
        />
        {namespaceDefinitionOptions[namespaceDefinition as NamespaceDefinitionType] ===
          namespaceDefinitionOptions.customformat && (
          <>
            {" - "}
            {namespaceFormat}
          </>
        )}
      </Text>
    </div>
  );
};

const DestinationPrefix: React.FC = () => {
  const {
    values: { prefix },
  } = useFormikContext<FormikConnectionFormValues>();

  return (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id="form.prefix" />:
      </Text>
      <Text size="md" color="grey">
        {prefix === "" ? (
          <FormattedMessage id="connectionForm.modal.destinationStreamNames.radioButton.mirror" />
        ) : (
          prefix
        )}
      </Text>
    </div>
  );
};

const NonBreakingChanges: React.FC<{
  allowAutoDetectSchema: boolean;
}> = ({ allowAutoDetectSchema }) => {
  const {
    values: { nonBreakingChangesPreference },
  } = useFormikContext<FormikConnectionFormValues>();
  const autoPropagationEnabled = useExperiment("autopropagation.enabled", true);
  const autoPropagationPrefix = autoPropagationEnabled ? "autopropagation." : "";
  const labelKey = autoPropagationEnabled
    ? "connectionForm.nonBreakingChangesPreference.autopropagation.label"
    : "connectionForm.nonBreakingChangesPreference.label";

  return allowAutoDetectSchema ? (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id={labelKey} />:
      </Text>
      <Text size="md" color="grey">
        <FormattedMessage
          id={`connectionForm.nonBreakingChangesPreference.${autoPropagationPrefix}${nonBreakingChangesPreference}`}
        />
      </Text>
    </div>
  ) : null;
};

export const ConnectionConfigurationFormPreview: React.FC = () => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  return (
    <FlexContainer className={styles.container}>
      <Frequency />
      <DestinationNamespace />
      <DestinationPrefix />
      <NonBreakingChanges allowAutoDetectSchema={allowAutoDetectSchema} />
    </FlexContainer>
  );
};
