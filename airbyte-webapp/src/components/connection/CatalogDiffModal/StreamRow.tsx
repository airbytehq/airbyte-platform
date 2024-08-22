import classnames from "classnames";
import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { StreamTransform } from "core/api/types/AirbyteClient";

import styles from "./StreamRow.module.scss";
import { DiffVerb } from "./types";

interface StreamRowProps {
  streamTransform: StreamTransform;
  syncMode?: string;

  diffVerb: DiffVerb;
}

export const SyncModeBox: React.FC<{ syncModeString: string }> = ({ syncModeString }) => {
  return <div className={styles.syncModeBox}> {syncModeString} </div>;
};

export const StreamRow: React.FC<StreamRowProps> = ({ streamTransform, syncMode, diffVerb }) => {
  const rowStyle = classnames(styles.row, {
    [styles.add]: diffVerb === "new",
    [styles.remove]: diffVerb === "removed",
  });

  const iconStyle = classnames(styles.icon, {
    [styles.plus]: diffVerb === "new",
    [styles.minus]: diffVerb === "removed",
    [styles.mod]: diffVerb === "changed",
  });

  const itemName = streamTransform.streamDescriptor.name;
  const namespace = streamTransform.streamDescriptor.namespace;
  const hasSyncModeChange = diffVerb === "removed" && syncMode;

  return (
    <tr className={rowStyle}>
      <td className={styles.nameCell}>
        <div className={styles.content}>
          <div className={styles.iconContainer}>
            {diffVerb === "new" ? (
              <Icon type="plus" className={iconStyle} />
            ) : diffVerb === "removed" ? (
              <Icon type="minus" className={iconStyle} />
            ) : (
              <Icon type="modification" className={iconStyle} />
            )}
          </div>
          <div title={namespace} className={styles.text}>
            {namespace ?? (
              <Text color="grey">
                <FormattedMessage id="general.dash" />
              </Text>
            )}
          </div>
        </div>
      </td>
      <td
        className={classnames(styles.nameCell, { [styles.lg]: !hasSyncModeChange })}
        colSpan={hasSyncModeChange ? 1 : 2}
      >
        <div title={itemName} className={styles.text}>
          {itemName}
        </div>
      </td>
      {hasSyncModeChange && (
        <td>
          <SyncModeBox syncModeString={syncMode} />
        </td>
      )}
    </tr>
  );
};
