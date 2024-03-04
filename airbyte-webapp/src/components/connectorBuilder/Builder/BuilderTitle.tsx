import classNames from "classnames";
import { useController } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderTitle.module.scss";

interface BuilderTitleProps {
  // path to the location in the Connector Manifest schema which should be set by this component
  path: string;
  label: string;
  size: "md" | "lg";
}

export const BuilderTitle: React.FC<BuilderTitleProps> = ({ path, label, size }) => {
  const { permission } = useConnectorBuilderFormState();
  const {
    field,
    fieldState: { error },
  } = useController({ name: path });
  const hasError = !!error;

  return (
    <div className={styles.container}>
      <Text className={styles.label} size="xs">
        {label}
      </Text>
      <Input
        disabled={permission === "readOnly"}
        containerClassName={classNames(styles.inputContainer, {
          [styles.md]: size === "md",
          [styles.lg]: size === "lg",
        })}
        className={classNames(styles.input, { [styles.md]: size === "md", [styles.lg]: size === "lg" })}
        {...field}
        type="text"
        value={field.value ?? ""}
        error={hasError}
      />
      {hasError && (
        <Text size="xs" className={styles.error}>
          <FormattedMessage id={error.message} />
        </Text>
      )}
    </div>
  );
};
