import { FormattedMessage } from "react-intl";

import { JobFailure } from "components/JobFailure";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { SchemaError as SchemaErrorType, LogsRequestError } from "core/api";

import styles from "./SchemaError.module.scss";

export const SchemaError = ({
  schemaError,
  refreshSchema,
}: {
  schemaError: Exclude<SchemaErrorType, null>;
  refreshSchema: () => Promise<void>;
}) => {
  const job = LogsRequestError.extractJobInfo(schemaError);

  return (
    <Card className={styles.card}>
      <FlexContainer direction="column">
        <FlexContainer justifyContent="flex-end">
          <Button type="button" onClick={refreshSchema} variant="secondary" icon={<Icon type="reset" />}>
            <FormattedMessage id="form.tryAgain" />
          </Button>
        </FlexContainer>
        <JobFailure job={job} fallbackMessage={schemaError.message} />
      </FlexContainer>
    </Card>
  );
};
