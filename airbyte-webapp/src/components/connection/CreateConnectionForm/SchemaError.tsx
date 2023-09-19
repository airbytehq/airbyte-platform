import { faRefresh } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage } from "react-intl";

import { JobFailure } from "components/JobFailure";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { LogsRequestError } from "core/request/LogsRequestError";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { SchemaError as SchemaErrorType } from "hooks/services/useSourceHook";

import styles from "./SchemaError.module.scss";

export const SchemaError = ({ schemaError }: { schemaError: Exclude<SchemaErrorType, null> }) => {
  const job = LogsRequestError.extractJobInfo(schemaError);
  const { refreshSchema } = useConnectionFormService();

  return (
    <Card className={styles.card}>
      <FlexContainer direction="column">
        <FlexContainer justifyContent="flex-end">
          <Button type="button" onClick={refreshSchema} variant="secondary" icon={<FontAwesomeIcon icon={faRefresh} />}>
            <FormattedMessage id="form.tryAgain" />
          </Button>
        </FlexContainer>
        <JobFailure job={job} fallbackMessage={schemaError.message} />
      </FlexContainer>
    </Card>
  );
};
