import { StatusIcon } from "components/ui/StatusIcon";

import { AttemptInfoRead, AttemptStatus } from "core/api/types/AirbyteClient";

interface AttemptStatusIconProps {
  attempt: AttemptInfoRead;
}

export const AttemptStatusIcon: React.FC<AttemptStatusIconProps> = ({ attempt }) => {
  if (attempt.attempt.status === AttemptStatus.failed) {
    return <StatusIcon status="error" />;
  } else if (attempt.attempt.status === AttemptStatus.running) {
    return <StatusIcon status="loading" />;
  } else if (attempt.attempt.status === AttemptStatus.succeeded) {
    return <StatusIcon status="success" />;
  }
  return null;
};
