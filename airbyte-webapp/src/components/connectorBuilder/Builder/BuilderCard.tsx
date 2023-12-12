import classNames from "classnames";
import React, { useState } from "react";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import styles from "./BuilderCard.module.scss";
import { BuilderStream, useBuilderWatch, BuilderState } from "../types";
import { useCopyValueIncludingArrays } from "../utils";

interface BuilderCardProps {
  className?: string;
  label?: string;
  tooltip?: string;
  toggleConfig?: {
    path: FieldPath<BuilderState>;
    defaultValue: unknown;
  };
  copyConfig?: {
    path: string;
    currentStreamIndex: number;
    copyToLabel: string;
    copyFromLabel: string;
  };
  docLink?: string;
}

export const BuilderCard: React.FC<React.PropsWithChildren<BuilderCardProps>> = ({
  children,
  className,
  toggleConfig,
  copyConfig,
  docLink,
  label,
  tooltip,
}) => {
  const { formatMessage } = useIntl();

  return (
    <Card className={classNames(className, styles.card)}>
      {(toggleConfig || label) && (
        <FlexContainer alignItems="center">
          <FlexItem grow>
            <FlexContainer>
              {toggleConfig && <CardToggle path={toggleConfig.path} defaultValue={toggleConfig.defaultValue} />}
              <ControlLabels
                className={classNames({ [styles.toggleLabel]: toggleConfig })}
                label={label}
                infoTooltipContent={tooltip}
                htmlFor={toggleConfig ? String(toggleConfig.path) : undefined}
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
      {toggleConfig ? <ToggledChildren path={toggleConfig.path}>{children}</ToggledChildren> : children}
    </Card>
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
        icon={<Icon type="import" />}
      />
      {currentRelevantConfig && (
        <Button
          variant="secondary"
          type="button"
          onClick={() => {
            setCopyToOpen(true);
          }}
          icon={<Icon type="share" />}
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
    <Modal size="sm" title={title} onClose={onCancel}>
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
    <Modal size="sm" title={title} onClose={onCancel}>
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
