import React, { Suspense, useCallback, useRef, useState } from "react";

import { Spinner } from "components/ui/Spinner";

import { SynchronousJobRead } from "core/request/AirbyteClient";

import { useAttemptLink } from "./attemptLinkUtils";
import ContentWrapper from "./components/ContentWrapper";
import { JobSummary } from "./components/JobSummary";
import styles from "./JobItem.module.scss";
import { JobsWithJobs } from "./types";
import { didJobSucceed, getJobAttempts, getJobId } from "./utils";

const ErrorDetails = React.lazy(() => import("./components/ErrorDetails"));
const JobLogs = React.lazy(() => import("./components/JobLogs"));

interface JobItemProps {
  job: SynchronousJobRead | JobsWithJobs;
}

export const JobItem: React.FC<JobItemProps> = ({ job }) => {
  const { jobId: linkedJobId } = useAttemptLink();
  const alreadyScrolled = useRef(false);
  const [isOpen, setIsOpen] = useState(() => linkedJobId === String(getJobId(job)));
  const scrollAnchor = useRef<HTMLDivElement>(null);

  const didSucceed = didJobSucceed(job);

  const onExpand = () => {
    setIsOpen((prevIsOpen) => !prevIsOpen);
  };

  const onDetailsToggled = useCallback(() => {
    if (alreadyScrolled.current || linkedJobId !== String(getJobId(job))) {
      return;
    }
    scrollAnchor.current?.scrollIntoView({
      block: "start",
    });
    alreadyScrolled.current = true;
  }, [job, linkedJobId]);

  return (
    <div className={styles.jobItem} ref={scrollAnchor}>
      <JobSummary isOpen={isOpen} isFailed={!didSucceed} onExpand={onExpand} job={job} attempts={getJobAttempts(job)} />
      <ContentWrapper isOpen={isOpen} onToggled={onDetailsToggled}>
        <div>
          <Suspense
            fallback={
              <div className={styles.jobItem__loading}>
                <Spinner size="sm" />
              </div>
            }
          >
            {isOpen && (
              <>
                <ErrorDetails attempts={getJobAttempts(job)} />
                <JobLogs job={job} jobIsFailed={!didSucceed} />
              </>
            )}
          </Suspense>
        </div>
      </ContentWrapper>
    </div>
  );
};
