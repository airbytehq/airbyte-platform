import { faSyncAlt } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Field } from "formik";
import React from "react";
import { FormattedMessage } from "react-intl";
import { useUnmount } from "react-use";

import { FormChangeTracker } from "components/common/FormChangeTracker";
import { Button } from "components/ui/Button";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

import { ConnectionConfigurationFormPreview } from "./ConnectionConfigurationFormPreview";
import styles from "./ConnectionFormFields.module.scss";
import { DestinationStreamPrefixName } from "./DestinationStreamPrefixName";
import { NamespaceDefinitionFieldNext } from "./NamespaceDefinitionFieldNext";
import { NonBreakingChangesPreferenceField } from "./NonBreakingChangesPreferenceField";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "./refreshSourceSchemaWithConfirmationOnDirty";
import { ScheduleField } from "./ScheduleField";
import { Section } from "./Section";
import { SyncCatalogField } from "./SyncCatalogField";

interface ConnectionFormFieldsProps {
  isSubmitting: boolean;
  dirty: boolean;
}

export const ConnectionFormFields: React.FC<ConnectionFormFieldsProps> = ({ isSubmitting, dirty }) => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  const { mode, formId } = useConnectionFormService();
  const { clearFormChange } = useFormChangeTrackerService();

  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(dirty);

  useUnmount(() => {
    clearFormChange(formId);
  });

  const isEditMode = mode === "edit";

  return (
    <>
      {/* FormChangeTracker is here as it has access to everything it needs without being repeated */}
      <FormChangeTracker changed={dirty} formId={formId} />
      <div className={styles.formContainer}>
        <Section
          title={<FormattedMessage id="form.configuration" />}
          collapsible={isEditMode}
          collapsedInitially={isEditMode}
          collapsedPreviewInfo={<ConnectionConfigurationFormPreview />}
          testId="configuration"
        >
          <ScheduleField />
          <NamespaceDefinitionFieldNext />
          <DestinationStreamPrefixName />
          {allowAutoDetectSchema && (
            <Field name="nonBreakingChangesPreference" component={NonBreakingChangesPreferenceField} />
          )}
        </Section>
        <Section flush flexHeight>
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
        </Section>
      </div>
    </>
  );
};
