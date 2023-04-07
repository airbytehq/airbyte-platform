import { FormattedNumber } from "react-intl";

import { Text } from "components/ui/Text";

interface FormattedCreditsProps {
  credits: number;
  color?: "green" | "grey300";
  size?: "sm" | "md";
}
export const FormattedCredits: React.FC<FormattedCreditsProps> = ({ credits, color, size }) => {
  return (
    <Text color={color} size={size} align="right" as="span">
      {credits < 0.005 && credits > 0 ? (
        <>
          {"<"}
          <FormattedNumber value={0.01} maximumFractionDigits={2} minimumFractionDigits={2} />
        </>
      ) : (
        <FormattedNumber value={credits} maximumFractionDigits={2} minimumFractionDigits={2} />
      )}
    </Text>
  );
};
