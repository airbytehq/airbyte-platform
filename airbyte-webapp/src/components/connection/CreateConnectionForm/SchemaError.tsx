import { FormattedMessage } from "react-intl";

import { JobFailure } from "components/JobFailure";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { ErrorWithJobInfo } from "core/api";

import styles from "./SchemaError.module.scss";

export const SchemaError = ({
  schemaError,
  refreshSchema,
}: {
  schemaError: Error;
  refreshSchema: () => Promise<void>;
}) => {
  const job = ErrorWithJobInfo.getJobInfo(schemaError);

  return (
    <Card className={styles.card}>
      <FlexContainer direction="column">
        <FlexContainer justifyContent="flex-end">
          <Button type="button" onClick={refreshSchema} variant="secondary" icon="reset">
            <FormattedMessage id="form.tryAgain" />
          </Button>
        </FlexContainer>
        <JobFailure job={job} fallbackMessage={schemaError.message} />
      </FlexContainer>
    </Card>
  );
};
