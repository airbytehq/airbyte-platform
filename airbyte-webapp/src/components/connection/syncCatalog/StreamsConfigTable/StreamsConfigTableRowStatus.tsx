import classNames from "classnames";
import { useState } from "react";
import { useUpdateEffect } from "react-use";

import { MinusIcon } from "components/icons/MinusIcon";
import { ModificationIcon } from "components/icons/ModificationIcon";
import { PlusIcon } from "components/icons/PlusIcon";

import { SyncSchemaStream } from "core/domain/catalog";

import styles from "./StreamsConfigTableRowStatus.module.scss";
import { StatusToDisplay, useStreamsConfigTableRowProps } from "./useStreamsConfigTableRowProps";

interface StreamsConfigTableRowStatusProps {
  stream: SyncSchemaStream;
  className?: string;
}

const getIcon = (statusToDisplay: StatusToDisplay): React.ReactNode | null => {
  switch (statusToDisplay) {
    case "added":
      return <PlusIcon />;
    case "removed":
      return <MinusIcon />;
    case "changed":
      return <ModificationIcon color="currentColor" />;
  }

  return null;
};

const VISIBLE_STATES: readonly StatusToDisplay[] = ["added", "changed", "removed"];

export const StreamsConfigTableRowStatus: React.FC<StreamsConfigTableRowStatusProps> = ({ stream, className }) => {
  const { statusToDisplay } = useStreamsConfigTableRowProps(stream);
  const [visibleStatus, setVisibleStatus] = useState(statusToDisplay);

  const isVisible = VISIBLE_STATES.includes(statusToDisplay);

  useUpdateEffect(() => {
    if (isVisible) {
      setVisibleStatus(statusToDisplay);
    }
  }, [statusToDisplay, isVisible]);

  const computedClassName = classNames(
    styles.icon,
    {
      [styles.plus]: visibleStatus === "added",
      [styles.minus]: visibleStatus === "removed",
      [styles.changed]: visibleStatus === "changed",
      [styles.visible]: isVisible,
    },
    className
  );

  return <div className={computedClassName}>{getIcon(visibleStatus)}</div>;
};
