import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";

import { BackfillFormField } from "./BackfillFormField";
import { ConnectionDataResidencyFormField } from "./ConnectionDataResidencyFormField";
import { ConnectionNameFormField } from "./ConnectionNameFormField";
import { ConnectionScheduleFormField } from "./ConnectionScheduleFormField";
import styles from "./ConnectionSettingsCard.module.scss";
import { ConnectionTagsFormField } from "./ConnectionTagsFormField";
import { DestinationNamespaceFormField } from "./DestinationNamespaceFormField";
import { DestinationStreamPrefixNameFormField } from "./DestinationStreamPrefixNameFormField";
import { RunOnDemandFormField } from "./RunOnDemandFormField";
import { SchemaChangeNotificationFormField } from "./SchemaChangeNotificationFormField";
import { SchemaChangesFormField } from "./SchemaChangesFormField";

interface ConnectionsSettingsCardProps {
  title: string;
  isCreating: boolean;
  hasConfiguredGeography?: boolean;
  source: SourceRead;
  destination: DestinationRead;
  isDeprecated?: boolean;
}

export const ConnectionsSettingsCard: React.FC<ConnectionsSettingsCardProps> = ({
  title,
  isCreating,
  hasConfiguredGeography = false,
  source,
  destination,
  isDeprecated = false,
}) => {
  const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataplanes);
  const isOnDemandCapacityEnabled = useFeature(FeatureItem.OnDemandCapacity);

  return (
    <Card title={title}>
      <FlexContainer direction="column" gap="xl">
        <ConnectionNameFormField />

        <ConnectionTagsFormField />
        <ConnectionScheduleFormField disabled={isDeprecated} />
        {isOnDemandCapacityEnabled && <RunOnDemandFormField disabled={isDeprecated} />}
        {isCreating && (
          <DestinationNamespaceFormField isCreating={isCreating} source={source} destination={destination} />
        )}
        {isCreating && <DestinationStreamPrefixNameFormField />}
      </FlexContainer>

      <Box mt="lg">
        {/* readonly mode disables all elements, including buttons, from the fieldset */}
        {/* to keep this toggle available, style and attribute a span like a button */}
        <span
          className={styles.advancedSettings}
          role="button"
          data-testid="advanced-settings-button"
          tabIndex={0}
          onClick={() => setIsAdvancedOpen((isAdvancedOpen) => !isAdvancedOpen)}
          onKeyUp={(e) =>
            (e.key === "Enter" || e.key === " ") && setIsAdvancedOpen((isAdvancedOpen) => !isAdvancedOpen)
          }
        >
          <FormattedMessage id="connectionForm.advancedSettings" />
          <Icon type={isAdvancedOpen ? "chevronDown" : "chevronRight"} size="md" />
        </span>

        {isCreating && (
          <Text color="grey">
            <FormattedMessage id="connectionForm.advancedSettings.subtitle" />
          </Text>
        )}

        {/* using styles.hidden to show/hide as residency field makes an http request for geographies */}
        {/* which triggers a suspense boundary - none of the places for a suspense fallback are good UX  */}
        {/* so always render, making the geography request as part of the initial page load */}
        <Box mt="xl">
          <FlexContainer direction="column" gap="xl" className={isAdvancedOpen ? undefined : styles.hidden}>
            {canEditDataGeographies && hasConfiguredGeography && (
              <ConnectionDataResidencyFormField disabled={isDeprecated} />
            )}
            {!isCreating && (
              <DestinationNamespaceFormField
                isCreating={isCreating}
                source={source}
                destination={destination}
                disabled={isDeprecated}
              />
            )}
            {!isCreating && <DestinationStreamPrefixNameFormField disabled={isDeprecated} />}
            <SchemaChangesFormField isCreating={isCreating} disabled={isDeprecated} />
            <SchemaChangeNotificationFormField disabled={isDeprecated} />
            <BackfillFormField disabled={isDeprecated} />
          </FlexContainer>
        </Box>

        {!isCreating && (
          <Box mt="xl">
            <FormSubmissionButtons reversed submitKey="form.saveChanges" />
          </Box>
        )}
      </Box>
    </Card>
  );
};
