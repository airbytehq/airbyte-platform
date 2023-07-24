import React from "react";

import Logs from "components/Logs";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { AttemptDetails } from "./AttemptDetails";
import DownloadButton from "./DownloadButton";
import { LinkToAttemptButton } from "./LinkToAttemptButton";
import { AttemptRead, JobDebugInfoRead } from "../../../core/request/AirbyteClient";

export const LogsDetails: React.FC<{
  jobId: string;
  path: string;
  currentAttempt?: AttemptRead;
  jobDebugInfo?: JobDebugInfoRead;
  showAttemptStats: boolean;
  logs?: string[];
}> = ({ path, jobId, currentAttempt, jobDebugInfo, showAttemptStats, logs }) => (
  <>
    {currentAttempt && showAttemptStats && (
      <Box p="md">
        <AttemptDetails attempt={currentAttempt} jobId={jobId} />
      </Box>
    )}
    <Box px="md" pt="md">
      <FlexContainer alignItems="center" gap="sm">
        <FlexItem grow>
          <Text size="sm" color="grey">
            {path}
          </Text>
        </FlexItem>
        <LinkToAttemptButton jobId={jobId} attemptId={currentAttempt?.id} />
        {jobDebugInfo && <DownloadButton jobDebugInfo={jobDebugInfo} fileName={`logs-${jobId}`} />}
      </FlexContainer>
    </Box>
    <Logs logsArray={logs} />
  </>
);
