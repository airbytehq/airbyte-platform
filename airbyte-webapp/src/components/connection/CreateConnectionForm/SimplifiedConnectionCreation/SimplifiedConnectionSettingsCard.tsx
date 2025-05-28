import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";

import { ConnectionTagsFormField } from "./ConnectionTagsFormField";
import { SimplfiedSchemaChangesFormField } from "./SimplfiedSchemaChangesFormField";
import { SimplifiedBackfillFormField } from "./SimplifiedBackfillFormField";
import { SimplfiedConnectionDataResidencyFormField } from "./SimplifiedConnectionDataResidencyFormField";
import { SimplifiedConnectionNameFormField } from "./SimplifiedConnectionNameFormField";
import { SimplifiedConnectionScheduleFormField } from "./SimplifiedConnectionScheduleFormField";
import styles from "./SimplifiedConnectionSettingsCard.module.scss";
import { SimplifiedDestinationNamespaceFormField } from "./SimplifiedDestinationNamespaceFormField";
import { SimplifiedDestinationStreamPrefixNameFormField } from "./SimplifiedDestinationStreamPrefixNameFormField";
import { SimplifiedSchemaChangeNotificationFormField } from "./SimplifiedSchemaChangeNotificationFormField";

interface SimplifiedConnectionsSettingsCardProps {
  title: string;
  isCreating: boolean;
  hasConfiguredGeography?: boolean;
  source: SourceRead;
  destination: DestinationRead;
  isDeprecated?: boolean;
}

export const SimplifiedConnectionsSettingsCard: React.FC<SimplifiedConnectionsSettingsCardProps> = ({
  title,
  isCreating,
  hasConfiguredGeography = false,
  source,
  destination,
  isDeprecated = false,
}) => {
  const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataplanes);

  return (
    <Card title={title}>
      <FlexContainer direction="column" gap="xl">
        <SimplifiedConnectionNameFormField />

        <ConnectionTagsFormField />
        <SimplifiedConnectionScheduleFormField disabled={isDeprecated} />
        {isCreating && (
          <SimplifiedDestinationNamespaceFormField isCreating={isCreating} source={source} destination={destination} />
        )}
        {isCreating && <SimplifiedDestinationStreamPrefixNameFormField />}
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
              <SimplfiedConnectionDataResidencyFormField disabled={isDeprecated} />
            )}
            {!isCreating && (
              <SimplifiedDestinationNamespaceFormField
                isCreating={isCreating}
                source={source}
                destination={destination}
                disabled={isDeprecated}
              />
            )}
            {!isCreating && <SimplifiedDestinationStreamPrefixNameFormField disabled={isDeprecated} />}
            <SimplfiedSchemaChangesFormField isCreating={isCreating} disabled={isDeprecated} />
            <SimplifiedSchemaChangeNotificationFormField disabled={isDeprecated} />
            <SimplifiedBackfillFormField disabled={isDeprecated} />
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
