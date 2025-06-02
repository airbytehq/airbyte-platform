import classNames from "classnames";
import { dump, load, YAMLException } from "js-yaml";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  useConnectorBuilderFormManagementState,
  useConnectorBuilderFormState,
} from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./BuilderCard.module.scss";
import { BuilderYamlField } from "./BuilderYamlField";
import { ManifestCompatibilityError } from "../convertManifestToBuilderForm";
import { BuilderState, BuilderStream, isYamlString } from "../types";
import { UiYamlToggleButton } from "../UiYamlToggleButton";
import { useBuilderWatch, useBuilderWatchWithPreview } from "../useBuilderWatch";
import { useCopyValueIncludingArrays, StreamFieldPath } from "../utils";

export interface BuilderCardProps {
  className?: string;
  label?: string;
  tooltip?: string;
  copyConfig?: {
    path: StreamFieldPath;
    currentStreamIndex: number;
    componentName: string;
  };
  docLink?: string;
  inputsConfig?: {
    toggleable: boolean;
    path: FieldPath<BuilderState>;
    defaultValue: unknown;
    yamlConfig?: {
      builderToManifest(builderValue: unknown): unknown;
      manifestToBuilder(manifestValue: unknown): unknown;
      getLockedInputKeys?(builderValue: unknown): string[];
    };
  };
  labelAction?: React.ReactNode;
  rightComponent?: React.ReactNode;
}

export const BuilderCard: React.FC<React.PropsWithChildren<BuilderCardProps>> = ({
  children,
  className,
  copyConfig,
  docLink,
  label,
  tooltip,
  inputsConfig,
  labelAction,
  rightComponent,
}) => {
  const { formatMessage } = useIntl();
  const { handleScrollToField } = useConnectorBuilderFormManagementState();

  const childElements = inputsConfig?.yamlConfig ? (
    <YamlEditableComponent
      path={inputsConfig.path}
      defaultValue={inputsConfig.defaultValue}
      builderToManifest={inputsConfig.yamlConfig.builderToManifest}
      manifestToBuilder={inputsConfig.yamlConfig.manifestToBuilder}
      getLockedInputKeys={inputsConfig.yamlConfig.getLockedInputKeys}
    >
      {children}
    </YamlEditableComponent>
  ) : (
    children
  );

  const elementRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (inputsConfig) {
      // Call handler in here to make sure it handles new scrollToField value from the context
      handleScrollToField(elementRef, inputsConfig.path);
    }
  }, [inputsConfig, handleScrollToField]);

  return (
    <Card className={className} bodyClassName={classNames(styles.card)} ref={elementRef}>
      {(inputsConfig?.toggleable || label || docLink || rightComponent) && (
        <FlexContainer alignItems="center">
          <FlexItem grow>
            <FlexContainer alignItems="center">
              {inputsConfig?.toggleable && (
                <CardToggle path={inputsConfig.path} defaultValue={inputsConfig.defaultValue} />
              )}
              <ControlLabels
                className={classNames(styles.label, { [styles.toggleLabel]: inputsConfig?.toggleable })}
                headerClassName={inputsConfig?.toggleable ? styles.toggleLabelHeader : undefined}
                label={label}
                labelAction={labelAction}
                infoTooltipContent={tooltip}
                htmlFor={inputsConfig ? String(inputsConfig.path) : undefined}
              />
            </FlexContainer>
          </FlexItem>
          {rightComponent}
          {docLink && (
            <a
              href={docLink}
              title={formatMessage({ id: "connectorBuilder.documentationLink" })}
              target="_blank"
              rel="noreferrer"
              className={styles.docLink}
            >
              <Icon type="docs" />
            </a>
          )}
        </FlexContainer>
      )}
      {copyConfig && <CopyButtons copyConfig={copyConfig} />}
      {inputsConfig?.toggleable ? (
        <ToggledChildren path={inputsConfig.path}>{childElements}</ToggledChildren>
      ) : (
        childElements
      )}
    </Card>
  );
};

interface YamlEditableComponentProps {
  path: FieldPath<BuilderState>;
  defaultValue: unknown;
  builderToManifest: (builderValue: unknown) => unknown;
  manifestToBuilder: (manifestValue: unknown) => unknown;
  getLockedInputKeys?: (builderValue: unknown) => string[];
}

const YamlEditableComponent: React.FC<React.PropsWithChildren<YamlEditableComponentProps>> = ({
  children,
  path,
  defaultValue,
  builderToManifest,
  manifestToBuilder,
  getLockedInputKeys,
}) => {
  const { resolveErrorMessage } = useConnectorBuilderFormState();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setValue, unregister } = useFormContext();
  const { fieldValue: formValue } = useBuilderWatchWithPreview(path);
  const pathString = path as string;
  const isYaml = isYamlString(formValue);
  const [previousUiValue, setPreviousUiValue] = useState(isYaml ? undefined : formValue);
  const [localYamlIsDirty, setLocalYamlIsDirty] = useState(false);
  const inputs = useBuilderWatch("formValues.inputs");

  const elementRef = useRef<HTMLDivElement | null>(null);
  const { handleScrollToField } = useConnectorBuilderFormManagementState();
  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(elementRef, pathString);
  }, [handleScrollToField, pathString]);

  const toggleLockedInputs = useCallback(
    (builderFormValue: unknown, setTo: "locked" | "unlocked") => {
      if (getLockedInputKeys) {
        const keysToToggle = getLockedInputKeys(builderFormValue);
        setValue(
          "formValues.inputs",
          inputs.map((input) =>
            keysToToggle.includes(input.key)
              ? {
                  ...input,
                  isLocked: setTo === "locked",
                }
              : input
          )
        );
      }
    },
    [getLockedInputKeys, inputs, setValue]
  );

  const confirmDiscardYaml = useCallback(
    (errorMessage?: string) => {
      const text = (
        <FlexContainer direction="column">
          <FlexItem>
            <FormattedMessage
              id={
                errorMessage
                  ? "connectorBuilder.yamlComponent.discardChanges.knownErrorIntro"
                  : "connectorBuilder.yamlComponent.discardChanges.unknownErrorIntro"
              }
            />
          </FlexItem>
          {errorMessage && (
            <Pre className={styles.discardYamlError} wrapText>
              {errorMessage}
            </Pre>
          )}
          <FlexItem>
            <FormattedMessage
              id={
                previousUiValue
                  ? "connectorBuilder.yamlComponent.discardChanges.errorOutro.uiValueAvailable"
                  : "connectorBuilder.yamlComponent.discardChanges.errorOutro.uiValueUnavailable"
              }
            />
          </FlexItem>
        </FlexContainer>
      );

      openConfirmationModal({
        title: "connectorBuilder.yamlComponent.discardChanges.title",
        text,
        submitButtonText: "connectorBuilder.yamlComponent.discardChanges.confirm",
        onSubmit: () => {
          const uiValue = previousUiValue ?? defaultValue;
          // lock the required inputs so they aren't duplicated when switching to UI
          toggleLockedInputs(uiValue, "locked");
          setValue(path, uiValue, {
            shouldValidate: true,
            shouldDirty: true,
            shouldTouch: true,
          });
          closeConfirmationModal();
        },
      });
    },
    [closeConfirmationModal, openConfirmationModal, path, previousUiValue, defaultValue, setValue, toggleLockedInputs]
  );

  return (
    <>
      {isYaml ? <BuilderYamlField path={path} setLocalYamlIsDirty={setLocalYamlIsDirty} /> : children}
      <UiYamlToggleButton
        className={styles.yamlEditorToggle}
        yamlSelected={isYaml}
        size="xs"
        disabled={localYamlIsDirty}
        onClick={() => {
          if (isYaml) {
            if (resolveErrorMessage) {
              confirmDiscardYaml(resolveErrorMessage);
              return;
            }

            let builderFormValue;
            try {
              const manifestValue = load(formValue);
              builderFormValue = manifestToBuilder(manifestValue);
            } catch (e) {
              const isKnownError = e instanceof ManifestCompatibilityError || e instanceof YAMLException;
              confirmDiscardYaml(isKnownError ? e.message : undefined);
              return;
            }

            // lock the required inputs so they aren't duplicated when switching to UI
            toggleLockedInputs(builderFormValue, "locked");
            setValue(path, builderFormValue, {
              shouldValidate: true,
              shouldDirty: true,
              shouldTouch: true,
            });
          } else {
            setPreviousUiValue(formValue);
            const manifestValue = builderToManifest(formValue);
            const yaml = dump(manifestValue);
            // unlock the locked inputs so they don't disappear when switching to YAML
            toggleLockedInputs(formValue, "unlocked");
            // unregister the path so that the YAML editor can properly register on mount
            unregister(path);
            setValue(path, yaml, {
              shouldValidate: true,
              shouldDirty: true,
              shouldTouch: true,
            });
          }
        }}
      />
    </>
  );
};

interface ToggledChildrenProps {
  path: FieldPath<BuilderState>;
}

const ToggledChildren: React.FC<React.PropsWithChildren<ToggledChildrenProps>> = ({ children, path }) => {
  const { fieldValue: value } = useBuilderWatchWithPreview(path);

  if (value !== undefined) {
    return <>{children}</>;
  }
  return null;
};

const CardToggle = ({ path, defaultValue }: { path: FieldPath<BuilderState>; defaultValue: unknown }) => {
  const { setValue, clearErrors } = useFormContext();
  const { fieldValue: value, isPreview } = useBuilderWatchWithPreview(path);
  return (
    <CheckBox
      id={path}
      data-testid={`toggle-${path}`}
      checked={value !== undefined}
      disabled={isPreview}
      onChange={(event) => {
        if (event.target.checked) {
          setValue(path, defaultValue);
        } else {
          setValue(path, undefined);
          clearErrors(path);
        }
      }}
    />
  );
};

const CopyButtons = ({ copyConfig }: Pick<BuilderCardProps, "copyConfig">) => {
  const { formatMessage } = useIntl();
  const [isCopyToOpen, setCopyToOpen] = useState(false);
  const [isCopyFromOpen, setCopyFromOpen] = useState(false);
  const copyValueIncludingArrays = useCopyValueIncludingArrays();
  const currentRelevantConfig = useWatch({
    name: copyConfig?.path ?? "",
    disabled: !copyConfig,
  });
  const streams = useBuilderWatch("formValues.streams");
  const otherStreamsWithSameRequestType = useMemo(() => {
    if (!copyConfig) {
      return [];
    }

    return streams.filter(
      (stream, index) =>
        index !== copyConfig.currentStreamIndex &&
        stream.requestType === streams[copyConfig.currentStreamIndex]?.requestType
    );
  }, [streams, copyConfig]);

  if (otherStreamsWithSameRequestType.length === 0 || !copyConfig) {
    return null;
  }

  return (
    <div className={styles.copyButtonContainer}>
      <Button
        variant="secondary"
        type="button"
        onClick={() => {
          setCopyFromOpen(true);
        }}
        icon="import"
      />
      {currentRelevantConfig && (
        <Button
          variant="secondary"
          type="button"
          onClick={() => {
            setCopyToOpen(true);
          }}
          icon="share"
        />
      )}
      {isCopyToOpen && (
        <CopyToModal
          onCancel={() => {
            setCopyToOpen(false);
          }}
          onApply={(selectedStreamIndices) => {
            selectedStreamIndices.forEach((selectedStreamIndex) => {
              copyValueIncludingArrays(copyConfig.currentStreamIndex, selectedStreamIndex, copyConfig.path, {
                shouldValidate: true,
              });
            });
            setCopyToOpen(false);
          }}
          currentStreamIndex={copyConfig.currentStreamIndex}
          title={formatMessage({ id: "connectorBuilder.copyToTitle" }, { componentName: copyConfig.componentName })}
        />
      )}
      {isCopyFromOpen && (
        <CopyFromModal
          onCancel={() => {
            setCopyFromOpen(false);
          }}
          onSelect={(selectedStreamIndex) => {
            copyValueIncludingArrays(selectedStreamIndex, copyConfig.currentStreamIndex, copyConfig.path, {
              shouldValidate: true,
            });
            setCopyFromOpen(false);
          }}
          currentStreamIndex={copyConfig.currentStreamIndex}
          title={formatMessage({ id: "connectorBuilder.copyFromTitle" }, { componentName: copyConfig.componentName })}
        />
      )}
    </div>
  );
};

function getStreamName(stream: BuilderStream) {
  return stream.name || <FormattedMessage id="connectorBuilder.emptyName" />;
}

const CopyToModal: React.FC<{
  onCancel: () => void;
  onApply: (selectedStreamIndices: number[]) => void;
  title: string;
  currentStreamIndex: number;
}> = ({ onCancel, onApply, title, currentStreamIndex }) => {
  const streams = useBuilderWatch("formValues.streams");
  const [selectMap, setSelectMap] = useState<Record<string, boolean>>({});
  return (
    <Modal size="sm" title={title} onCancel={onCancel}>
      <form
        onSubmit={() => {
          onApply(
            Object.entries(selectMap)
              .filter(([, selected]) => selected)
              .map(([index]) => Number(index))
          );
        }}
      >
        <ModalBody className={styles.modalStreamListContainer}>
          {streams.map((stream, index) => (
            <label
              htmlFor={`copy-to-stream-${index}`}
              key={index}
              className={classNames(styles.toggleContainer, {
                // hide these options instead of filtering out, because we still need the right stream indexes to be used in onApply
                [styles.hiddenCopyOption]:
                  stream.requestType !== streams[currentStreamIndex].requestType || index === currentStreamIndex,
              })}
            >
              <CheckBox
                id={`copy-to-stream-${index}`}
                checked={selectMap[index] || false}
                onChange={() => {
                  setSelectMap({ ...selectMap, [index]: !selectMap[index] });
                }}
              />
              <Text>{getStreamName(stream)}</Text>
            </label>
          ))}
        </ModalBody>
        <ModalFooter>
          <Button variant="secondary" onClick={onCancel}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button type="submit" disabled={Object.values(selectMap).filter(Boolean).length === 0}>
            <FormattedMessage id="form.apply" />
          </Button>
        </ModalFooter>
      </form>
    </Modal>
  );
};

const CopyFromModal: React.FC<{
  onCancel: () => void;
  onSelect: (selectedStreamIndex: number) => void;
  title: string;
  currentStreamIndex: number;
}> = ({ onCancel, onSelect, title, currentStreamIndex }) => {
  const streams = useBuilderWatch("formValues.streams");
  return (
    <Modal size="sm" title={title} onCancel={onCancel}>
      <ModalBody className={styles.modalStreamListContainer}>
        {streams.map((stream, index) => (
          <button
            key={index}
            onClick={() => {
              onSelect(index);
            }}
            className={classNames(styles.streamItem, {
              // hide these options instead of filtering out, because we still need the right stream indexes to be used in onSelect
              [styles.hiddenCopyOption]:
                stream.requestType !== streams[currentStreamIndex].requestType || index === currentStreamIndex,
            })}
          >
            <Text>{getStreamName(stream)}</Text>
          </button>
        ))}
      </ModalBody>
    </Modal>
  );
};
