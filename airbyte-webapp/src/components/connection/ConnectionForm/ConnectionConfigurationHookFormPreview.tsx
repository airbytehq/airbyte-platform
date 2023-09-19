import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionScheduleType } from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

// we can use styles from Formik`s version of this component
import styles from "./ConnectionConfigurationFormPreview.module.scss";
import { HookFormConnectionFormValues } from "./hookFormConfig";
import { namespaceDefinitionOptions } from "./types";

const Frequency: React.FC = () => {
  const { getValues } = useFormContext<HookFormConnectionFormValues>();
  const { scheduleType, scheduleData } = getValues();

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
  const { getValues } = useFormContext<HookFormConnectionFormValues>();
  const { namespaceDefinition, namespaceFormat } = getValues();

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
  const { getValues } = useFormContext<HookFormConnectionFormValues>();
  const { prefix } = getValues();

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

const NonBreakingChanges: React.FC = () => {
  const { getValues } = useFormContext<HookFormConnectionFormValues>();
  const { nonBreakingChangesPreference } = getValues();

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

export const ConnectionConfigurationHookFormPreview: React.FC = () => {
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
