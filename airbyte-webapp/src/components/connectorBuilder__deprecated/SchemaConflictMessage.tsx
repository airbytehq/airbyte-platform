import { FormattedMessage } from "react-intl";

import styles from "./SchemaConflictMessage.module.scss";

export const SchemaConflictMessage: React.FC<{ errors?: string[] }> = ({ errors }) => (
  <>
    <FormattedMessage id="connectorBuilder.differentSchemaDescription" />
    {errors && (
      <>
        <br />
        <br />
        <FormattedMessage id="connectorBuilder.incompatibleChanges" />
        <ul className={styles.container}>
          {errors.map((error) => (
            <li key={error}>{error}</li>
          ))}
        </ul>
      </>
    )}
  </>
);
