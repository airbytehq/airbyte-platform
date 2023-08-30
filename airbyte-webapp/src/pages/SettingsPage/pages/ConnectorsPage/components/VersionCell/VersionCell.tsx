import classNames from "classnames";
import React from "react";
import { useWatch } from "react-hook-form";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FlexContainer } from "components/ui/Flex";

import { ReleaseStage } from "core/request/AirbyteClient";

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
  releaseStage?: ReleaseStage;
}

export const VersionCell: React.FC<VersionCellProps> = ({
  connectorDefinitionId,
  onChange,
  currentVersion,
  latestVersion,
  releaseStage,
}) => (
  <Form<ConnectorVersionFormValues>
    defaultValues={{
      id: connectorDefinitionId,
      version: latestVersion || currentVersion,
    }}
    schema={versionCellFormSchema}
    onSubmit={onChange}
  >
    <VersionFormContent
      releaseStage={releaseStage}
      connectorDefinitionId={connectorDefinitionId}
      currentVersion={currentVersion}
      latestVersion={latestVersion}
    />
  </Form>
);

const VersionFormContent = ({
  releaseStage,
  connectorDefinitionId,
  currentVersion,
  latestVersion,
}: {
  releaseStage?: ReleaseStage;
  connectorDefinitionId: string;
  currentVersion: string;
  latestVersion?: string;
}) => {
  const { formatMessage } = useIntl();
  const value = useWatch({ name: "version" });

  const inputLatestNote =
    value === latestVersion && releaseStage !== ReleaseStage.custom
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
