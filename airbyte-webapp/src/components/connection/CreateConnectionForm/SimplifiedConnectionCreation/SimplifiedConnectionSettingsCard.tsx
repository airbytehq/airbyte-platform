import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

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
  sourceName: string;
  isDeprecated?: boolean;
}

export const SimplifiedConnectionsSettingsCard: React.FC<SimplifiedConnectionsSettingsCardProps> = ({
  title,
  isCreating,
  sourceName,
  isDeprecated = false,
}) => {
  const [isAdvancedOpen, setIsAdvancedOpen] = useState(false);
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  const canBackfillNewColumns = useExperiment("platform.auto-backfill-on-new-columns", false);

  return (
    <Card title={title} className={styles.hideOverflow}>
      <FlexContainer direction="column" gap="xl">
        <SimplifiedConnectionNameFormField />
        <SimplifiedConnectionScheduleFormField disabled={isDeprecated} />
        {isCreating && <SimplifiedDestinationNamespaceFormField isCreating={isCreating} sourceName={sourceName} />}
        {isCreating && <SimplifiedDestinationStreamPrefixNameFormField />}
      </FlexContainer>

      <Box mt="md">
        {/* readonly mode disables all elements, including buttons, from the fieldset */}
        {/* to keep this toggle available, style and attribute a span like a button */}
        <span
          className={styles.advancedSettings}
          role="button"
          tabIndex={0}
          onClick={() => setIsAdvancedOpen((isAdvancedOpen) => !isAdvancedOpen)}
          onKeyUp={(e) =>
            (e.key === "Enter" || e.key === " ") && setIsAdvancedOpen((isAdvancedOpen) => !isAdvancedOpen)
          }
        >
          <FormattedMessage id="connectionForm.advancedSettings" />
          <Icon type={isAdvancedOpen ? "chevronDown" : "chevronRight"} size="lg" />
        </span>

        {isCreating && (
          <Text color="grey">
            <FormattedMessage id="connectionForm.advancedSettings.subtitle" />
          </Text>
        )}

        {isAdvancedOpen && <hr className={styles.hr} />}

        {/* using styles.hidden to show/hide as residency field makes an http request for geographies */}
        {/* which triggers a suspense boundary - none of the places for a suspense fallback are good UX  */}
        {/* so always render, making the geography request as part of the initial page load */}
        <FlexContainer direction="column" gap="xl" className={isAdvancedOpen ? undefined : styles.hidden}>
          {canEditDataGeographies && <SimplfiedConnectionDataResidencyFormField disabled={isDeprecated} />}
          {!isCreating && (
            <SimplifiedDestinationNamespaceFormField
              isCreating={isCreating}
              sourceName={sourceName}
              disabled={isDeprecated}
            />
          )}
          {!isCreating && <SimplifiedDestinationStreamPrefixNameFormField disabled={isDeprecated} />}
          <SimplfiedSchemaChangesFormField isCreating={isCreating} disabled={isDeprecated} />
          <SimplifiedSchemaChangeNotificationFormField disabled={isDeprecated} />
          {canBackfillNewColumns && <SimplifiedBackfillFormField disabled={isDeprecated} />}
        </FlexContainer>

        {!isCreating && (
          <Box mt="xl">
            <FormSubmissionButtons reversed submitKey="form.saveChanges" />
          </Box>
        )}
      </Box>
    </Card>
  );
};
