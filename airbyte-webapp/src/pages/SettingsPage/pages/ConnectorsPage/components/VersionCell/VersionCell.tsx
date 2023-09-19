import classNames from "classnames";
import React from "react";
import { useWatch } from "react-hook-form";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FlexContainer } from "components/ui/Flex";

import { SubmissionButton } from "./SubmissionButton";
import styles from "./VersionCell.module.scss";

const versionCellFormSchema = yup.object().shape({
  id: yup.string().trim().required("form.empty.error"),
  version: yup.string().trim().required("form.empty.error"),
});

export interface ConnectorVersionFormValues {
  id: string;
  version: string;
}

export interface VersionCellProps {
  connectorDefinitionId: string;
  onChange: (values: ConnectorVersionFormValues) => Promise<void>;
  currentVersion: string;
  latestVersion?: string;
  custom?: boolean;
}

export const VersionCell: React.FC<VersionCellProps> = ({
  connectorDefinitionId,
  onChange,
  currentVersion,
  latestVersion,
  custom,
}) => (
  <Form<ConnectorVersionFormValues>
    defaultValues={{
      id: connectorDefinitionId,
      version: latestVersion || currentVersion,
    }}
    reinitializeDefaultValues
    schema={versionCellFormSchema}
    onSubmit={onChange}
  >
    <VersionFormContent
      custom={custom}
      connectorDefinitionId={connectorDefinitionId}
      currentVersion={currentVersion}
      latestVersion={latestVersion}
    />
  </Form>
);

const VersionFormContent = ({
  custom,
  connectorDefinitionId,
  currentVersion,
  latestVersion,
}: {
  custom?: boolean;
  connectorDefinitionId: string;
  currentVersion: string;
  latestVersion?: string;
}) => {
  const { formatMessage } = useIntl();
  const value = useWatch({ name: "version" });

  const inputLatestNote =
    value === latestVersion && !custom
      ? formatMessage({
          id: "admin.latestNote",
        })
      : undefined;

  return (
    <FlexContainer justifyContent="flex-end" alignItems="center" className={styles.versionCell}>
      <div className={styles.inputField} data-before={inputLatestNote}>
        <FormControl
          name="version"
          fieldType="input"
          className={classNames(styles.versionInput, { [styles.noLatest]: inputLatestNote === undefined })}
          containerControlClassName={styles.inputContainer}
          type="text"
          autoComplete="off"
        />
      </div>
      <SubmissionButton
        connectorDefinitionId={connectorDefinitionId}
        currentVersion={currentVersion}
        latestVersion={latestVersion}
      />
    </FlexContainer>
  );
};
