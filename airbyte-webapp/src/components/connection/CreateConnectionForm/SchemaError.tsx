import { faRefresh } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage } from "react-intl";

import { JobFailure } from "components/JobFailure";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { LogsRequestError } from "core/request/LogsRequestError";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useConnectionHookFormService } from "hooks/services/ConnectionForm/ConnectionHookFormService";
import { useExperiment } from "hooks/services/Experiment";
import { SchemaError as SchemaErrorType } from "hooks/services/useSourceHook";

import styles from "./SchemaError.module.scss";

export const SchemaError = ({ schemaError }: { schemaError: Exclude<SchemaErrorType, null> }) => {
  const job = LogsRequestError.extractJobInfo(schemaError);

  /**
   *TODO: remove after successful CreateConnectionForm migration
   *https://github.com/airbytehq/airbyte-platform-internal/issues/8639
   */
  const doUseCreateConnectionHookForm = useExperiment("form.createConnectionHookForm", false);
  const useConnectionFormContextProvider = doUseCreateConnectionHookForm
    ? useConnectionHookFormService
    : useConnectionFormService;
  const { refreshSchema } = useConnectionFormContextProvider();

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
