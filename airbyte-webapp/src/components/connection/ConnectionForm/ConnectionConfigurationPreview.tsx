import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionConfigurationPreview.module.scss";
import { FormConnectionFormValues } from "./formConfig";
import { namespaceDefinitionOptions } from "./types";

const Frequency: React.FC = () => {
  const { control } = useFormContext<FormConnectionFormValues>();

  const scheduleType = useWatch({ name: "scheduleType", control });
  const scheduleData = useWatch({ name: "scheduleData", control });

  return (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id="form.frequency" />:
      </Text>
      <Text size="md" color="grey">
        {scheduleType === ConnectionScheduleType.manual && <FormattedMessage id="frequency.manual" />}
        {scheduleType === ConnectionScheduleType.cron && (
          <FormattedMessage
            id="frequency.cronPreview"
            values={{ cronString: scheduleData?.cron?.cronExpression, cronTimezone: scheduleData?.cron?.cronTimeZone }}
          />
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
  const { control } = useFormContext<FormConnectionFormValues>();

  const namespaceDefinition = useWatch({ name: "namespaceDefinition", control });
  const namespaceFormat = useWatch({ name: "namespaceFormat", control });
  const namespaceDefinitionValue = namespaceDefinitionOptions[namespaceDefinition];

  return (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id="connectionForm.namespaceDefinition.title" />:
      </Text>
      <Text size="md" color="grey">
        {namespaceDefinitionValue === namespaceDefinitionOptions.destination && (
          <FormattedMessage id="connectionForm.destinationFormat" />
        )}
        {namespaceDefinitionValue === namespaceDefinitionOptions.source && (
          <FormattedMessage id="connectionForm.sourceFormat" />
        )}
        {namespaceDefinitionValue === namespaceDefinitionOptions.customformat && (
          <FormattedMessage id="connectionForm.customFormatPreview" values={{ customFormat: namespaceFormat }} />
        )}
      </Text>
    </div>
  );
};

const DestinationPrefix: React.FC = () => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const prefix = useWatch({ name: "prefix", control });

  return (
    <div>
      <Text size="xs" color="grey">
        <FormattedMessage id="form.prefix" />:
      </Text>
      <Text size="md" color="grey">
        {prefix ? prefix : <FormattedMessage id="connectionForm.modal.destinationStreamNames.radioButton.mirror" />}
      </Text>
    </div>
  );
};

const NonBreakingChanges: React.FC = () => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const nonBreakingChangesPreference = useWatch({ name: "nonBreakingChangesPreference", control });

  const autoPropagationEnabled = useExperiment("autopropagation.enabled", true);
  const autoPropagationPrefix = autoPropagationEnabled ? "autopropagation." : "";
  const labelKey = autoPropagationEnabled
    ? "connectionForm.nonBreakingChangesPreference.autopropagation.label"
    : "connectionForm.nonBreakingChangesPreference.label";

  return (
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
  );
};

export const ConnectionConfigurationPreview: React.FC = () => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  return (
    <FlexContainer className={styles.container}>
      <Frequency />
      <DestinationNamespace />
      <DestinationPrefix />
      {allowAutoDetectSchema && <NonBreakingChanges />}
    </FlexContainer>
  );
};
