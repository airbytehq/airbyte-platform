import { faArrowUpRightFromSquare } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { useField, useFormikContext } from "formik";
import get from "lodash/get";
import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import styles from "./BuilderCard.module.scss";
import { BuilderStream } from "../types";

interface BuilderCardProps {
  className?: string;
  label?: React.ReactNode;
  toggleConfig?: {
    toggledOn: boolean;
    onToggle: (newToggleValue: boolean) => void;
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
}) => {
  const { formatMessage } = useIntl();
  const { setFieldValue, getFieldMeta } = useFormikContext();
  const [isCopyToOpen, setCopyToOpen] = useState(false);
  const [isCopyFromOpen, setCopyFromOpen] = useState(false);
  const streams = getFieldMeta<BuilderStream[]>("streams").value;
  return (
    <Card className={classNames(className, styles.card)}>
      {(toggleConfig || label) && (
        <FlexContainer alignItems="center">
          <FlexItem grow>
            <FlexContainer>
              {toggleConfig && (
                <CheckBox
                  data-testid="toggle"
                  checked={toggleConfig.toggledOn}
                  onChange={(event) => {
                    toggleConfig.onToggle(event.target.checked);
                  }}
                />
              )}
              <span>{label}</span>
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
              <Icon type="docs" size="lg" />
            </a>
          )}
        </FlexContainer>
      )}
      {copyConfig && streams.length > 1 && (
        <div className={styles.copyButtonContainer}>
          <Button
            variant="secondary"
            type="button"
            onClick={() => {
              setCopyFromOpen(true);
            }}
            icon={<FontAwesomeIcon icon={faArrowUpRightFromSquare} rotation={180} />}
          />
          {get(streams[copyConfig.currentStreamIndex], copyConfig.path) && (
            <Button
              variant="secondary"
              type="button"
              onClick={() => {
                setCopyToOpen(true);
              }}
              icon={<FontAwesomeIcon icon={faArrowUpRightFromSquare} />}
            />
          )}
          {isCopyToOpen && (
            <CopyToModal
              onCancel={() => {
                setCopyToOpen(false);
              }}
              onApply={(selectedStreamIndices) => {
                const sectionToCopy = getFieldMeta(
                  `streams[${copyConfig.currentStreamIndex}].${copyConfig.path}`
                ).value;
                selectedStreamIndices.forEach((index) => {
                  setFieldValue(`streams[${index}].${copyConfig.path}`, sectionToCopy, true);
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
                setFieldValue(
                  `streams[${copyConfig.currentStreamIndex}].${copyConfig.path}`,
                  getFieldMeta(`streams[${selectedStreamIndex}].${copyConfig.path}`).value,
                  true
                );
                setCopyFromOpen(false);
              }}
              currentStreamIndex={copyConfig.currentStreamIndex}
              title={copyConfig.copyFromLabel}
            />
          )}
        </div>
      )}
      {(!toggleConfig || toggleConfig.toggledOn) && children}
    </Card>
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
  const [streams] = useField<BuilderStream[]>("streams");
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
          {streams.value.map((stream, index) =>
            index === currentStreamIndex ? null : (
              <label htmlFor={`copy-to-stream-${index}`} key={index} className={styles.toggleContainer}>
                <CheckBox
                  id={`copy-to-stream-${index}`}
                  checked={selectMap[index] || false}
                  onChange={() => {
                    setSelectMap({ ...selectMap, [index]: !selectMap[index] });
                  }}
                />
                {getStreamName(stream)}
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
  const [streams] = useField<BuilderStream[]>("streams");
  return (
    <Modal size="sm" title={title} onClose={onCancel}>
      <ModalBody className={styles.modalStreamListContainer}>
        {streams.value.map((stream, index) =>
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
