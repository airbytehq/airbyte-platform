import { FormattedMessage } from "react-intl";
import { InferType } from "yup";

import { Text } from "components/ui/Text";

import { userInEventSchema } from "../types";

interface TimelineEventUserProps {
  user?: InferType<typeof userInEventSchema>;
}

interface UserCancelledDescriptionProps {
  user?: InferType<typeof userInEventSchema>;
  jobType: string;
}

export const TimelineEventUser: React.FC<TimelineEventUserProps> = ({ user }) => {
  return <>{user?.name?.trim() || user?.email?.trim() || <FormattedMessage id="connection.timeline.user.unknown" />}</>;
};

export const UserCancelledDescription: React.FC<UserCancelledDescriptionProps> = ({ user, jobType }) => {
  const messageId =
    jobType === "sync"
      ? `connection.timeline.sync_cancelled.description`
      : jobType === "clear"
      ? `connection.timeline.clear_cancelled.description`
      : jobType === "refresh"
      ? `connection.timeline.refresh_cancelled.description`
      : null;

  if (!messageId) {
    return null;
  }

  return (
    <>
      <Text as="span" color="grey400" size="sm">
        <FormattedMessage
          id={messageId}
          values={{
            user: <TimelineEventUser user={user} />,
          }}
        />
      </Text>
      {jobType !== "clear" && (
        <Text as="span" color="grey400" size="sm">
          |
        </Text>
      )}
    </>
  );
};
