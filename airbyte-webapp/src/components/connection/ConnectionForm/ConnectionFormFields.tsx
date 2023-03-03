import { faSyncAlt } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Field, FieldProps } from "formik";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useUnmount } from "react-use";

import { ControlLabels } from "components";
import { FormChangeTracker } from "components/common/FormChangeTracker";
import { Button } from "components/ui/Button";
import { Input } from "components/ui/Input";

import { NamespaceDefinitionType } from "core/request/AirbyteClient";
import { useNewTableDesignExperiment } from "hooks/connection/useNewTableDesignExperiment";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ValuesProps } from "hooks/services/useConnectionHook";

import { ConnectionConfigurationFormPreview } from "./ConnectionConfigurationFormPreview";
import styles from "./ConnectionFormFields.module.scss";
import { DestinationStreamPrefixName } from "./DestinationStreamPrefixName";
import { FormikConnectionFormValues } from "./formConfig";
import { FormFieldLayout } from "./FormFieldLayout";
import { NamespaceDefinitionField } from "./NamespaceDefinitionField";
import { NamespaceDefinitionFieldNext } from "./NamespaceDefinitionFieldNext";
import { NonBreakingChangesPreferenceField } from "./NonBreakingChangesPreferenceField";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "./refreshSourceSchemaWithConfirmationOnDirty";
import { ScheduleField } from "./ScheduleField";
import { Section } from "./Section";
import { SyncCatalogField } from "./SyncCatalogField";

interface ConnectionFormFieldsProps {
  values: ValuesProps | FormikConnectionFormValues;
  isSubmitting: boolean;
  dirty: boolean;
}

export const ConnectionFormFields: React.FC<ConnectionFormFieldsProps> = ({ values, isSubmitting, dirty }) => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  const { mode, formId } = useConnectionFormService();
  const { formatMessage } = useIntl();
  const { clearFormChange } = useFormChangeTrackerService();

  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(dirty);

  useUnmount(() => {
    clearFormChange(formId);
  });

  const isNewTableDesignEnabled = useNewTableDesignExperiment();

  return (
    <>
      {/* FormChangeTracker is here as it has access to everything it needs without being repeated */}
      <FormChangeTracker changed={dirty} formId={formId} />
      <div className={styles.formContainer}>
        <Section
          title={<FormattedMessage id="form.configuration" />}
          collapsible={isNewTableDesignEnabled && mode === "edit"}
          collapsedInitially
          collapsedPreviewInfo={<ConnectionConfigurationFormPreview />}
          testId="configuration"
        >
          <ScheduleField />
          {isNewTableDesignEnabled && (
            <>
              <NamespaceDefinitionFieldNext />
              <DestinationStreamPrefixName />
            </>
          )}
          {allowAutoDetectSchema && (
            <Field name="nonBreakingChangesPreference" component={NonBreakingChangesPreferenceField} />
          )}
        </Section>
        {!isNewTableDesignEnabled && (
          <Section title={<FormattedMessage id="connection.streams" />}>
            <Field name="namespaceDefinition" component={NamespaceDefinitionField} />
            {values.namespaceDefinition === NamespaceDefinitionType.customformat && (
              <Field name="namespaceFormat">
                {({ field, meta }: FieldProps<string>) => (
                  <FormFieldLayout>
                    <ControlLabels
                      className={styles.namespaceFormatLabel}
                      nextLine
                      error={!!meta.error}
                      label={<FormattedMessage id="connectionForm.namespaceFormat.title" />}
                      infoTooltipContent={<FormattedMessage id="connectionForm.namespaceFormat.subtitle" />}
                    />
                    <Input
                      {...field}
                      error={!!meta.error}
                      disabled={isSubmitting || mode === "readonly"}
                      placeholder={formatMessage({
                        id: "connectionForm.namespaceFormat.placeholder",
                      })}
                    />
                  </FormFieldLayout>
                )}
              </Field>
            )}
            <Field name="prefix">
              {({ field }: FieldProps<string>) => (
                <FormFieldLayout>
                  <ControlLabels
                    nextLine
                    optional
                    label={formatMessage({
                      id: "form.prefix",
                    })}
                    infoTooltipContent={formatMessage({
                      id: "form.prefix.message",
                    })}
                  />
                  <Input
                    {...field}
                    type="text"
                    disabled={isSubmitting || mode === "readonly"}
                    placeholder={formatMessage({
                      id: `form.prefix.placeholder`,
                    })}
                    data-testid="prefixInput"
                  />
                </FormFieldLayout>
              )}
            </Field>
          </Section>
        )}
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
