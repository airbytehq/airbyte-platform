import dayjs from "dayjs";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { AttemptRead } from "core/request/AirbyteClient";

import { getFailureFromAttempt, isCancelledAttempt } from "../utils";

interface IProps {
  attempts?: AttemptRead[];
}

const ErrorDetails: React.FC<IProps> = ({ attempts }) => {
  const { formatMessage } = useIntl();

  if (!attempts?.length) {
    return null;
  }

  const getInternalFailureMessage = (attempt: AttemptRead) => {
    const failure = getFailureFromAttempt(attempt);
    const failureMessage = failure?.internalMessage ?? formatMessage({ id: "errorView.unknown" });

    return `${formatMessage({
      id: "sources.additionalFailureInfo",
    })}: ${failureMessage}`;
  };

  const attempt = attempts[attempts.length - 1];
  const failure = getFailureFromAttempt(attempt);
  const isCancelled = isCancelledAttempt(attempt);

  if (!failure || isCancelled) {
    return null;
  }

  const internalMessage = getInternalFailureMessage(attempt);
  return (
    <Box pl="2xl" py="md">
      <FlexContainer gap="xs">
        {!!failure.timestamp && (
          <Text size="sm" color="grey" italicized>
            {dayjs.utc(failure.timestamp).format("YYYY-MM-DD HH:mm:ss")} -
          </Text>
        )}
        <Text size="sm" color="grey">
          {internalMessage}
        </Text>
      </FlexContainer>
    </Box>
  );
};

export default ErrorDetails;
