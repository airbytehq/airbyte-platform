import React from "react";
import styled from "styled-components";

import Logs from "components/Logs";

import { AttemptDetails } from "./AttemptDetails";
import DownloadButton from "./DownloadButton";
import { LinkToAttemptButton } from "./LinkToAttemptButton";
import { AttemptRead, JobDebugInfoRead } from "../../../core/request/AirbyteClient";

const LogHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 4px;
  margin-top: 9px;
  font-size: 12px;
  padding: 0 10px;
`;

const AttemptDetailsSection = styled.div`
  padding: 10px 0 10px 10px;
`;

const LogPath = styled.span`
  flex: 1;
  color: ${({ theme }) => theme.greyColor40};
`;

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
      <AttemptDetailsSection>
        <AttemptDetails attempt={currentAttempt} jobId={jobId} />
      </AttemptDetailsSection>
    )}
    <LogHeader>
      <LogPath>{path}</LogPath>
      <LinkToAttemptButton jobId={jobId} attemptId={currentAttempt?.id} />
      {jobDebugInfo && <DownloadButton jobDebugInfo={jobDebugInfo} fileName={`logs-${jobId}`} />}
    </LogHeader>
    <Logs logsArray={logs} />
  </>
);
