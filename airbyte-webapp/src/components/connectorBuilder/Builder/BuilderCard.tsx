import classNames from "classnames";
import { dump, load, YAMLException } from "js-yaml";
import debounce from "lodash/debounce";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { YamlEditor } from "components/connectorBuilder/YamlEditor";
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
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderCard.module.scss";
import { ManifestCompatibilityError } from "../convertManifestToBuilderForm";
import { BuilderState, BuilderStream, isYamlString, useBuilderWatch } from "../types";
import { UiYamlToggleButton } from "../UiYamlToggleButton";
import { useCopyValueIncludingArrays } from "../utils";

interface BuilderCardProps {
  className?: string;
  label?: string;
  tooltip?: string;
  copyConfig?: {
    path: string;
    currentStreamIndex: number;
    copyToLabel: string;
    copyFromLabel: string;
  };
  docLink?: string;
  inputsConfig?: {
    toggleable: boolean;
    path: FieldPath<BuilderState>;
    defaultValue: unknown;
    yamlConfig?: {
      builderToManifest(builderValue: unknown): unknown;
      manifestToBuilder(manifestValue: unknown): unknown;
    };
  };
}

export const BuilderCard: React.FC<React.PropsWithChildren<BuilderCardProps>> = ({
  children,
  className,
  copyConfig,
  docLink,
  label,
  tooltip,
  inputsConfig,
}) => {
  const { formatMessage } = useIntl();

  const childElements = inputsConfig?.yamlConfig ? (
    <YamlEditableComponent
      path={inputsConfig.path}
      defaultValue={inputsConfig.defaultValue}
      builderToManifest={inputsConfig.yamlConfig.builderToManifest}
      manifestToBuilder={inputsConfig.yamlConfig.manifestToBuilder}
    >
      {children}
    </YamlEditableComponent>
  ) : (
    children
  );

  return (
    <Card className={className} bodyClassName={classNames(styles.card)}>
      {(inputsConfig?.toggleable || label || docLink) && (
        <FlexContainer alignItems="center">
          <FlexItem grow>
            <FlexContainer>
              {inputsConfig?.toggleable && (
                <CardToggle path={inputsConfig.path} defaultValue={inputsConfig.defaultValue} />
              )}
              <ControlLabels
                className={classNames(styles.label, { [styles.toggleLabel]: inputsConfig?.toggleable })}
                label={label}
                infoTooltipContent={tooltip}
                htmlFor={inputsConfig ? String(inputsConfig.path) : undefined}
              />
            </FlexContainer>
          </FlexItem>
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
  builderToManifest(builderValue: unknown): unknown;
  manifestToBuilder(manifestValue: unknown): unknown;
}

const YamlEditableComponent: React.FC<React.PropsWithChildren<YamlEditableComponentProps>> = ({
  children,
  path,
  defaultValue,
  builderToManifest,
  manifestToBuilder,
}) => {
  const { resolveErrorMessage } = useConnectorBuilderFormState();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setValue, register } = useFormContext();
  const formValue = useBuilderWatch(path);
  const pathString = path as string;
  const isYaml = isYamlString(formValue);
  const [previousUiValue, setPreviousUiValue] = useState(isYaml ? defaultValue : formValue);
  // Use a separate state for the YamlEditor value to avoid the debouncedSetValue
  // causing the YamlEditor be set to a previous value while still typing
  const [localYamlValue, setLocalYamlValue] = useState(isYaml ? formValue : "");
  const [localYamlIsDirty, setLocalYamlIsDirty] = useState(false);
  const debouncedSetValue = useMemo(
    () =>
      debounce((...args: Parameters<typeof setValue>) => {
        setValue(...args);
        setLocalYamlIsDirty(false);
      }, 500),
    [setValue]
  );

  const elementRef = useRef<HTMLDivElement | null>(null);
  const { handleScrollToField } = useConnectorBuilderFormManagementState();
  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(elementRef, pathString);
  }, [handleScrollToField, pathString]);

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
          {errorMessage && <Pre className={styles.discardYamlError}>{errorMessage}</Pre>}
          <FlexItem>
            <FormattedMessage id="connectorBuilder.yamlComponent.discardChanges.errorOutro" />
          </FlexItem>
        </FlexContainer>
      );

      openConfirmationModal({
        title: "connectorBuilder.yamlComponent.discardChanges.title",
        text,
        submitButtonText: "connectorBuilder.yamlComponent.discardChanges.confirm",
        onSubmit: () => {
          setValue(path, previousUiValue, {
            shouldValidate: true,
            shouldDirty: true,
            shouldTouch: true,
          });
          closeConfirmationModal();
        },
      });
    },
    [closeConfirmationModal, openConfirmationModal, path, previousUiValue, setValue]
  );

  return (
    <>
      {isYaml ? (
        <div
          className={styles.yamlEditor}
          ref={(ref) => {
            elementRef.current = ref;
            // Call handler in here to make sure it handles new refs
            handleScrollToField(elementRef, path);
          }}
        >
          <YamlEditor
            value={localYamlValue}
            onChange={(val: string | undefined) => {
              setLocalYamlValue(val ?? "");
              setLocalYamlIsDirty(true);
              debouncedSetValue(path, val, {
                shouldValidate: true,
                shouldDirty: true,
                shouldTouch: true,
              });
            }}
            onMount={(_) => {
              // register path so that validation rules are applied
              register(path);
            }}
          />
        </div>
      ) : (
        children
      )}
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

            setValue(path, builderFormValue, {
              shouldValidate: true,
              shouldDirty: true,
              shouldTouch: true,
            });
          } else {
            setPreviousUiValue(formValue);
            const manifestValue = builderToManifest(formValue);
            const yaml = dump(manifestValue);
            setLocalYamlValue(yaml);
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
  const value = useBuilderWatch(path);

  if (value !== undefined) {
    return <>{children}</>;
  }
  return null;
};

const CardToggle = ({ path, defaultValue }: { path: FieldPath<BuilderState>; defaultValue: unknown }) => {
  const { setValue, clearErrors } = useFormContext();
  const value = useBuilderWatch(path);

  return (
    <CheckBox
      id={path}
      data-testid={`toggle-${path}`}
      checked={value !== undefined}
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
  const [isCopyToOpen, setCopyToOpen] = useState(false);
  const [isCopyFromOpen, setCopyFromOpen] = useState(false);
  const copyValueIncludingArrays = useCopyValueIncludingArrays();
  const streams = useBuilderWatch("formValues.streams");
  const currentRelevantConfig = useWatch({
    name: `formValues.streams.${copyConfig?.currentStreamIndex}.${copyConfig?.path}`,
    disabled: !copyConfig,
  });
  if (streams.length <= 1 || !copyConfig) {
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
          title={copyConfig.copyToLabel}
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
          title={copyConfig.copyFromLabel}
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
          {streams.map((stream, index) =>
            index === currentStreamIndex ? null : (
              <label htmlFor={`copy-to-stream-${index}`} key={index} className={styles.toggleContainer}>
                <CheckBox
                  id={`copy-to-stream-${index}`}
                  checked={selectMap[index] || false}
                  onChange={() => {
                    setSelectMap({ ...selectMap, [index]: !selectMap[index] });
                  }}
                />
                <Text>{getStreamName(stream)}</Text>
              </label>
            )
          )}
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
        {streams.map((stream, index) =>
          currentStreamIndex === index ? null : (
            <button
              key={index}
              onClick={() => {
                onSelect(index);
              }}
              className={styles.streamItem}
            >
              <Text>{getStreamName(stream)}</Text>
            </button>
          )
        )}
      </ModalBody>
    </Modal>
  );
};
