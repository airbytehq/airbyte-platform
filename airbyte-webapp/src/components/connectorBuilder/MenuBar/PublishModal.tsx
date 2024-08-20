import classNames from "classnames";
import debounce from "lodash/debounce";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { ReactMarkdown } from "react-markdown/lib/react-markdown";
import { useUpdateEffect } from "react-use";
import * as yup from "yup";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { LabelInfo } from "components/Label";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import {
  GENERATE_CONTRIBUTION_NOTIFICATION_ID,
  useBuilderCheckContribution,
  useBuilderGenerateContribution,
  useGetBuilderProjectBaseImage,
  useListBuilderProjectVersions,
} from "core/api";
import { CheckContributionRead } from "core/api/types/ConnectorBuilderClient";
import { useFormatError } from "core/errors";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { NON_I18N_ERROR_TYPE } from "core/utils/form";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";
import {
  useConnectorBuilderFormState,
  convertJsonToYaml,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./PublishModal.module.scss";
import { useBuilderWatch } from "../types";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

const PUBLISH_TO_WORKSPACE_NOTIFICATION_ID = "publish-to-workspace-notification";

export type PublishType = "workspace" | "marketplace";

export const PublishModal: React.FC<{ onClose: () => void; initialPublishType: PublishType }> = ({
  onClose,
  initialPublishType = "workspace",
}) => {
  const [publishType, setPublishType] = useState(initialPublishType);

  return (
    <Modal size="md" title={<FormattedMessage id="connectorBuilder.publishConnector" />} onCancel={onClose}>
      {publishType === "workspace" ? (
        <PublishToWorkspace onClose={onClose} setPublishType={setPublishType} />
      ) : (
        <ContributeToMarketplace onClose={onClose} setPublishType={setPublishType} />
      )}
    </Modal>
  );
};

const PublishTypeSwitcher: React.FC<{
  selectedPublishType: PublishType;
  setPublishType: (type: PublishType) => void;
}> = ({ selectedPublishType, setPublishType }) => {
  const analyticsService = useAnalyticsService();
  const { streamNames } = useConnectorBuilderFormState();
  const { getStreamTestWarnings } = useStreamTestMetadata();

  const streamsWithWarnings = useMemo(() => {
    return streamNames.filter((streamName) => getStreamTestWarnings(streamName).length > 0);
  }, [getStreamTestWarnings, streamNames]);
  const isMarketplaceContributionActionDisabled = streamsWithWarnings.length > 0;

  return (
    <FlexContainer>
      <RadioButtonTiles<PublishType>
        name="publishType"
        options={[
          {
            value: "workspace",
            label: <FormattedMessage id="connectorBuilder.publishModal.toWorkspace.label" />,
            description: <FormattedMessage id="connectorBuilder.publishModal.toWorkspace.description" />,
          },
          {
            value: "marketplace",
            label: <FormattedMessage id="connectorBuilder.publishModal.toMarketplace.label" />,
            description: <FormattedMessage id="connectorBuilder.publishModal.toMarketplace.description" />,
            disabled: isMarketplaceContributionActionDisabled,
            tooltipContent: isMarketplaceContributionActionDisabled ? (
              <FormattedMessage id="connectorBuilder.publishModal.toMarketplace.disabledDescription" />
            ) : null,
          },
        ]}
        selectedValue={selectedPublishType}
        onSelectRadioButton={(publishType) => {
          setPublishType(publishType);
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.PUBLISH_RADIO_BUTTON_SELECTED, {
            actionDescription: "A radio button in the Publish modal was selected",
            selectedPublishType: publishType,
          });
        }}
      />
    </FlexContainer>
  );
};

interface InnerModalProps {
  onClose: () => void;
  setPublishType: (type: PublishType) => void;
}

interface PublishToWorkspaceFormValues {
  name: string;
  description?: string;
  useVersion: boolean;
  version: number;
}

const PublishToWorkspace: React.FC<InnerModalProps> = ({ onClose, setPublishType }) => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const {
    projectId,
    jsonManifest: manifest,
    currentProject,
    publishProject,
    releaseNewVersion,
  } = useConnectorBuilderFormState();
  const { data: versions, isLoading: isLoadingVersions } = useListBuilderProjectVersions(currentProject);
  const connectorName = useBuilderWatch("name");
  const { setValue } = useFormContext();

  const minVersion = versions && versions.length > 0 ? Math.max(...versions.map((version) => version.version)) + 1 : 1;

  const schema = useMemo(
    () =>
      yup.object().shape({
        name: yup.string().required("form.empty.error").max(256, "connectorBuilder.maxLength"),
        description: yup.string().max(256, "connectorBuilder.maxLength"),
        useVersion: yup.bool().required(),
        version: yup.number().min(minVersion).required(),
      }),
    [minVersion]
  );

  const handleSubmit = async (values: PublishToWorkspaceFormValues) => {
    unregisterNotificationById(PUBLISH_TO_WORKSPACE_NOTIFICATION_ID);
    try {
      if (currentProject.sourceDefinitionId) {
        await releaseNewVersion({
          manifest,
          description: values.description ?? "",
          sourceDefinitionId: currentProject.sourceDefinitionId,
          projectId: currentProject.id,
          useAsActiveVersion: values.useVersion,
          version: values.version,
        });
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.RELEASE_NEW_PROJECT_VERSION, {
          actionDescription: "User released a new version of a Connector Builder project",
          projectVersion: values.version,
          projectId: currentProject.id,
        });
      } else {
        await publishProject({
          manifest,
          name: values.name,
          description: values.description ?? "",
          projectId,
        });
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.PUBLISH_PROJECT, {
          actionDescription: "User published a Connector Builder project",
          projectId: currentProject.id,
        });
      }

      // push name change upstream so it's updated
      setValue("name", values.name);

      registerNotification({
        id: PUBLISH_TO_WORKSPACE_NOTIFICATION_ID,
        text: (
          <FormattedMessage
            id="connectorBuilder.publishedMessage"
            values={{ name: values.name, version: values.version }}
          />
        ),
        type: "success",
      });
      onClose();
    } catch (e) {
      registerNotification({
        id: PUBLISH_TO_WORKSPACE_NOTIFICATION_ID,
        text: <FormattedMessage id="form.error" values={{ message: e.message }} />,
        type: "error",
      });
    }
  };

  const publishTypeSwitcher = <PublishTypeSwitcher selectedPublishType="marketplace" setPublishType={setPublishType} />;

  const isMarketplaceContributionEnabled = useExperiment("connectorBuilder.contributeToMarketplace", false);

  if (isLoadingVersions) {
    return (
      <ModalBody>
        <FlexContainer justifyContent="center" direction="column">
          {publishTypeSwitcher}
          <Spinner />
        </FlexContainer>
      </ModalBody>
    );
  }

  return (
    <Form<PublishToWorkspaceFormValues>
      defaultValues={{
        name: connectorName,
        description: "",
        useVersion: true,
        version: minVersion,
      }}
      schema={schema}
      onSubmit={handleSubmit}
    >
      <ModalBody>
        <FlexContainer direction="column" gap="xl">
          {isMarketplaceContributionEnabled && (
            <PublishTypeSwitcher selectedPublishType="workspace" setPublishType={setPublishType} />
          )}
          <Message
            text={
              <FormattedMessage
                id={
                  currentProject.sourceDefinitionId
                    ? "connectorBuilder.releaseNewVersionDescription"
                    : "connectorBuilder.publishDescription"
                }
              />
            }
          />
          <FlexContainer direction="column" gap="none">
            <FormControl<PublishToWorkspaceFormValues>
              name="name"
              fieldType="input"
              label="Connector Name"
              containerControlClassName={styles.formControl}
            />
            <FlexContainer direction="row">
              {currentProject.sourceDefinitionId && (
                <div className={styles.versionInput}>
                  <FormControl<PublishToWorkspaceFormValues>
                    name="version"
                    fieldType="input"
                    label="Version"
                    disabled
                  />
                </div>
              )}
              <FormControl<PublishToWorkspaceFormValues>
                name="description"
                fieldType="textarea"
                label="Description"
                optional
                containerControlClassName={styles.formControl}
              />
            </FlexContainer>
          </FlexContainer>
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <FlexItem grow>
          <FlexContainer justifyContent="space-between">
            {currentProject.sourceDefinitionId && (
              <FormControl<PublishToWorkspaceFormValues>
                name="useVersion"
                fieldType="switch"
                label={formatMessage({ id: "connectorBuilder.setAsActiveVersion.label" })}
                labelTooltip={formatMessage({ id: "connectorBuilder.setAsActiveVersion.tooltip" })}
                containerControlClassName={classNames(styles.formControl, styles.useVersion)}
              />
            )}
            <FormSubmissionButtons
              justify="flex-end"
              submitKey="connectorBuilder.publish"
              onCancelClickCallback={onClose}
              allowNonDirtySubmit
              allowNonDirtyCancel
            />
          </FlexContainer>
        </FlexItem>
      </ModalFooter>
    </Form>
  );
};

interface ContributeToMarketplaceFormValues {
  name: string;
  connectorImageName: string;
  description?: string;
  githubToken: string;
}

const ContributeToMarketplace: React.FC<InnerModalProps> = ({ onClose, setPublishType }) => {
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const formatError = useFormatError();
  const connectorName = useBuilderWatch("name");
  const connectorImageName = useMemo(() => convertConnectorNameToImageName(connectorName), [connectorName]);
  const { jsonManifest, updateYamlCdkVersion } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");

  // update the version so that the manifest reflects which CDK version was used to build it
  const jsonManifestWithCorrectedVersion = useMemo(
    () => updateYamlCdkVersion(jsonManifest),
    [jsonManifest, updateYamlCdkVersion]
  );

  const {
    data: baseImageRead,
    error: baseImageError,
    isLoading: isLoadingBaseImage,
  } = useGetBuilderProjectBaseImage({ manifest: jsonManifestWithCorrectedVersion });

  const [imageNameError, setImageNameError] = useState<string | null>(null);
  const { mutateAsync: generateContribution, isLoading: isSubmittingContribution } = useBuilderGenerateContribution();

  const publishTypeSwitcher = <PublishTypeSwitcher selectedPublishType="marketplace" setPublishType={setPublishType} />;
  const [showPublishWarning, setShowPublishWarning] = useLocalStorage("connectorBuilderPublishWarning", true);

  if (isLoadingBaseImage) {
    return (
      <ModalBody>
        <FlexContainer justifyContent="center" direction="column">
          {publishTypeSwitcher}
          <Spinner />
        </FlexContainer>
      </ModalBody>
    );
  }

  const baseImage = baseImageRead?.baseImage;
  if (!baseImage) {
    return (
      <ModalBody>
        <FlexContainer justifyContent="center" direction="column">
          {publishTypeSwitcher}
          <Message
            type="error"
            text={
              <>
                <FormattedMessage id="connectorBuilder.contribution.modal.baseImageNotFound" />
                {!!baseImageError && (
                  <>
                    {" "}
                    <FormattedMessage
                      id="connectorBuilder.contribution.modal.baseImageNotFound.withError"
                      values={{ error: formatError(baseImageError as Error) }}
                    />
                  </>
                )}
              </>
            }
          />
        </FlexContainer>
      </ModalBody>
    );
  }

  const handleSubmit = async (values: ContributeToMarketplaceFormValues) => {
    unregisterNotificationById(GENERATE_CONTRIBUTION_NOTIFICATION_ID);

    const jsonManifestWithDescription = {
      ...jsonManifestWithCorrectedVersion,
      description: values.description,
    };
    const yamlManifest = convertJsonToYaml(jsonManifestWithDescription);

    const contribution = await generateContribution({
      name: values.name,
      connector_image_name: values.connectorImageName,
      description: values.description,
      github_token: values.githubToken,
      manifest_yaml: convertJsonToYaml(jsonManifestWithDescription),
      base_image: baseImage,
    });
    registerNotification({
      id: GENERATE_CONTRIBUTION_NOTIFICATION_ID,
      type: "success",
      timeout: false,
      text: (
        <FormattedMessage
          id="connectorBuilder.contribution.success"
          values={{
            name: values.name,
            a: (node: React.ReactNode) => (
              <a href={contribution.pull_request_url} target="_blank" rel="noreferrer">
                {node}
              </a>
            ),
          }}
        />
      ),
    });

    // push name change upstream so it's updated
    setValue("name", values.name);

    // save description to manifest
    if (mode === "yaml") {
      setValue("yaml", yamlManifest);
    } else {
      setValue("formValues.description", values.description);
    }

    onClose();
  };

  return (
    <Form<ContributeToMarketplaceFormValues>
      defaultValues={{
        name: connectorName,
        connectorImageName,
        description: jsonManifest.description,
        githubToken: "",
      }}
      schema={yup.object().shape({
        name: yup.string().required("form.empty.error"),
        connectorImageName: yup
          .string()
          .required("form.empty.error")
          .test((value, { createError }) => {
            if (!value) {
              return createError({ message: "form.empty.error" });
            }
            if (imageNameError) {
              return createError({ message: imageNameError, type: NON_I18N_ERROR_TYPE });
            }
            return true;
          }),
        description: yup.string(),
        githubToken: yup.string().required("form.empty.error"),
      })}
      onSubmit={handleSubmit}
    >
      <ModalBody>
        <FlexContainer direction="column" gap="xl">
          <PublishTypeSwitcher selectedPublishType="marketplace" setPublishType={setPublishType} />
          {showPublishWarning && (
            <Message
              type="warning"
              text={<FormattedMessage id="connectorBuilder.warnPublishSecrets" />}
              onClose={() => {
                setShowPublishWarning(false);
              }}
            />
          )}
          <FlexContainer direction="column" gap="none">
            <FormControl<ContributeToMarketplaceFormValues>
              name="name"
              fieldType="input"
              label={formatMessage({ id: "connectorBuilder.contribution.modal.connectorName.label" })}
              containerControlClassName={styles.formControl}
            />
            <ConnectorImageNameInput
              connectorName={connectorName}
              imageNameError={imageNameError}
              setImageNameError={setImageNameError}
            />
            <FormControl<ContributeToMarketplaceFormValues>
              name="description"
              fieldType="textarea"
              label={formatMessage({ id: "connectorBuilder.contribution.modal.description.label" })}
              labelTooltip={
                <LabelInfo
                  label={<FormattedMessage id="connectorBuilder.contribution.modal.description.label" />}
                  description={<FormattedMessage id="connectorBuilder.contribution.modal.description.tooltip" />}
                  examples={["source-google-sheets", "source-big-query"]}
                />
              }
              optional
              containerControlClassName={styles.formControl}
            />
            <FormControl<ContributeToMarketplaceFormValues>
              name="githubToken"
              fieldType="input"
              type="password"
              label={formatMessage({ id: "connectorBuilder.contribution.modal.githubToken.label" })}
              labelTooltip={
                <LabelInfo
                  label={<FormattedMessage id="connectorBuilder.contribution.modal.githubToken.label" />}
                  description={
                    <ReactMarkdown>
                      {formatMessage({ id: "connectorBuilder.contribution.modal.githubToken.tooltip" })}
                    </ReactMarkdown>
                  }
                />
              }
              containerControlClassName={styles.formControl}
              description={
                <FlexContainer justifyContent="space-between" alignItems="center">
                  <Text size="sm" color="grey400">
                    <FormattedMessage id="connectorBuilder.contribution.modal.githubToken.subText" />
                  </Text>
                  <ExternalLink
                    href="https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic"
                    className={styles.githubTokenLink}
                    variant="primary"
                  >
                    <Icon type="export" />
                    <FormattedMessage id="connectorBuilder.contribution.modal.githubToken.docsLink" />
                  </ExternalLink>
                </FlexContainer>
              }
            />
          </FlexContainer>
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <FlexContainer direction="column" alignItems="flex-end">
          <FormSubmissionButtons
            justify="flex-end"
            submitKey="connectorBuilder.contribute"
            onCancelClickCallback={onClose}
            allowNonDirtySubmit
            allowNonDirtyCancel
          />
          {isSubmittingContribution && (
            <Text size="sm" color="grey600">
              <FormattedMessage id="connectorBuilder.contribution.modal.submittingToGithub" />
            </Text>
          )}
        </FlexContainer>
      </ModalFooter>
    </Form>
  );
};

const imageNameIsValid = (imageName: string) => {
  return /^source-[a-z0-9-]+$/.test(imageName);
};

const convertConnectorNameToImageName = (connectorName: string) => {
  return `source-${connectorName
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+$/, "")}`;
};

const ConnectorImageNameInput: React.FC<{
  connectorName: string;
  imageNameError: string | null;
  setImageNameError: (error: string | null) => void;
}> = ({ connectorName, imageNameError, setImageNameError }) => {
  const { formatMessage } = useIntl();
  const fieldName = "connectorImageName";

  const { trigger } = useFormContext();
  const [footer, setFooter] = useState<string | null>(null);
  const { getCachedCheck, fetchContributionCheck } = useBuilderCheckContribution();

  const updateErrorAndFooter = useCallback(
    (contributionCheck: CheckContributionRead) => {
      if (contributionCheck.connector_exists) {
        setImageNameError(
          contributionCheck?.connector_name
            ? formatMessage(
                { id: "connectorBuilder.contribution.modal.connectorAlreadyExistsWithName" },
                { name: contributionCheck.connector_name }
              )
            : formatMessage({ id: "connectorBuilder.contribution.modal.connectorAlreadyExistsWithoutName" })
        );
        setFooter(null);
      } else {
        setImageNameError(null);
        setFooter(
          formatMessage({ id: "connectorBuilder.contribution.modal.connectorDoesNotExist" }, { name: connectorName })
        );
      }
    },
    [connectorName, formatMessage, setImageNameError]
  );

  // An async function that debounces the call to fetchContributionCheck and resolves
  // the promise with the result so that the function can be awaited.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const debouncedCheckContribution = useCallback(
    debounce(async (imageName: string, resolve: (checkResult: CheckContributionRead | Error) => void) => {
      const result = await fetchContributionCheck({ connector_image_name: imageName });
      resolve(result);
    }, 1000),
    [fetchContributionCheck]
  );

  const imageName = useWatch({ name: fieldName });
  useEffect(() => {
    // cancel any pending checks to avoid stale results
    debouncedCheckContribution.cancel();

    if (!imageName) {
      // don't need to set footer or error state, because the yup validation will take precedence here
      return;
    }

    if (!imageNameIsValid(imageName)) {
      setImageNameError(formatMessage({ id: "connectorBuilder.contribution.modal.invalidImageName" }));
      setFooter(null);
      return;
    }

    const cachedCheck = getCachedCheck({ connector_image_name: imageName });
    if (cachedCheck) {
      updateErrorAndFooter(cachedCheck);
      return;
    }

    setImageNameError(null);
    setFooter(null);

    debouncedCheckContribution(imageName, (result) => {
      // in the case of an error response don't show any footer or error message
      if (result instanceof Error) {
        return;
      }
      updateErrorAndFooter(result);
    });
  }, [debouncedCheckContribution, formatMessage, getCachedCheck, imageName, setImageNameError, updateErrorAndFooter]);

  // when imageNameError changes, trigger validation of the image name field
  useUpdateEffect(() => {
    trigger(fieldName);
  }, [imageNameError, trigger]);

  return (
    <FormControl<ContributeToMarketplaceFormValues>
      name={fieldName}
      fieldType="input"
      label={formatMessage({ id: "connectorBuilder.contribution.modal.connectorImageName.label" })}
      labelTooltip={
        <LabelInfo
          label={<FormattedMessage id="connectorBuilder.contribution.modal.connectorImageName.label" />}
          description={
            <ReactMarkdown>
              {formatMessage({ id: "connectorBuilder.contribution.modal.connectorImageName.tooltip" })}
            </ReactMarkdown>
          }
          examples={["source-google-sheets", "source-big-query"]}
        />
      }
      containerControlClassName={styles.formControl}
      footer={footer ?? undefined}
    />
  );
};
