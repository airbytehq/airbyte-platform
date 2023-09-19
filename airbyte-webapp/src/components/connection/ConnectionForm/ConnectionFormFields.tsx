import { faSyncAlt } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Field } from "formik";
import React from "react";
import { FormattedMessage } from "react-intl";
import { useEffectOnce } from "react-use";

import { FormChangeTracker } from "components/common/FormChangeTracker";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CollapsibleCard } from "components/ui/CollapsibleCard";
import { FlexContainer } from "components/ui/Flex";

import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { ConnectionConfigurationFormPreview } from "./ConnectionConfigurationFormPreview";
import styles from "./ConnectionFormFields.module.scss";
import { DestinationStreamPrefixName } from "./DestinationStreamPrefixName";
import { NamespaceDefinitionField } from "./NamespaceDefinitionField";
import { NonBreakingChangesPreferenceField } from "./NonBreakingChangesPreferenceField";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "./refreshSourceSchemaWithConfirmationOnDirty";
import { ScheduleField } from "./ScheduleField";
import { SyncCatalogField } from "./SyncCatalogField";

interface ConnectionFormFieldsProps {
  isSubmitting: boolean;
  dirty: boolean;
  validateForm?: () => void;
}

export const ConnectionFormFields: React.FC<ConnectionFormFieldsProps> = ({ isSubmitting, dirty, validateForm }) => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  const { mode, formId } = useConnectionFormService();

  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(dirty);

  // If the source doesn't select any streams by default, the initial untouched state
  // won't validate that at least one is selected. In this case, a user could submit the form
  // without selecting any streams, which would trigger an error and cause a lousy UX.
  useEffectOnce(() => {
    validateForm?.();
  });

  const isEditMode = mode === "edit";

  return (
    <>
      {/* FormChangeTracker is here as it has access to everything it needs without being repeated */}
      <FormChangeTracker changed={dirty} formId={formId} />
      <FlexContainer direction="column">
        <CollapsibleCard
          title={<FormattedMessage id="form.configuration" />}
          collapsible={isEditMode}
          defaultCollapsedState={isEditMode}
          collapsedPreviewInfo={<ConnectionConfigurationFormPreview />}
          testId="configuration"
        >
          <FlexContainer direction="column" gap="lg">
            <ScheduleField />
            <NamespaceDefinitionField />
            <DestinationStreamPrefixName />
            {allowAutoDetectSchema && (
              <Field name="nonBreakingChangesPreference" component={NonBreakingChangesPreferenceField} />
            )}
          </FlexContainer>
        </CollapsibleCard>
        <Card>
          <Field
            name="syncCatalog.streams"
            component={SyncCatalogField}
            isSubmitting={isSubmitting}
            additionalControl={
              <Button
                onClick={refreshSchema}
                type="button"
                variant="secondary"
                data-testid="refresh-source-schema-btn"
                disabled={isSubmitting}
              >
                <FontAwesomeIcon icon={faSyncAlt} className={styles.tryArrow} />
                <FormattedMessage id="connection.updateSchema" />
              </Button>
            }
          />
        </Card>
      </FlexContainer>
    </>
  );
};
