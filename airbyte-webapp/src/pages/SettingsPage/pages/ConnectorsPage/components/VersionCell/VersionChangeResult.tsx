import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

export const VersionChangeResult: React.FC<{ feedback?: string }> = ({ feedback }) => {
  const { isDirty } = useFormState();

  if (feedback === "success" && !isDirty) {
    return <FormattedMessage id="form.savedChange" />;
  }
  if (feedback && feedback !== "success") {
    return (
      <Text as="span" color="red">
        {feedback}
      </Text>
    );
  }

  return null;
};
