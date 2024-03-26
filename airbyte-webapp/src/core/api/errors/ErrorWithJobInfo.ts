import { SynchronousJobRead } from "../types/AirbyteClient";

/**
 * An error that is linked to a synchronous job that ran (e.g. connector configuration check or discover schema)
 * and has the job information attached to it.
 */
export class ErrorWithJobInfo extends Error {
  constructor(
    message: string,
    public readonly jobInfo: SynchronousJobRead
  ) {
    super(message);
  }

  /**
   * Extract the job info from an error in case it is an ErrorWithJobInfo.
   */
  static getJobInfo(error: Error | null): SynchronousJobRead | null {
    return error instanceof ErrorWithJobInfo ? error.jobInfo : null;
  }
}
