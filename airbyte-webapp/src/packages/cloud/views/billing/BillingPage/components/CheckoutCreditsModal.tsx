import { useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, FormattedNumber, useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useStripeCheckout } from "core/api/cloud";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { ModalContentProps } from "hooks/services/Modal";

import { STRIPE_SUCCESS_QUERY } from "./RemainingCredits";

const DEFAULT_CREDITS = 200;
const MIN_CREDITS = 20;
const MAX_CREDITS = 6000;

interface CreditsFormValues {
  quantity: number;
}

const getPrice = (quantity: number) => {
  if (quantity >= 4800) {
    return 2.08;
  }
  if (quantity >= 2200) {
    return 2.27;
  }

  return 2.5;
};

const PricePreview: React.FC = () => {
  const { formatNumber } = useIntl();
  const quantity = useWatch<CreditsFormValues>({ name: "quantity" });
  return (
    <Text size="md">
      <FlexContainer direction="column">
        <FlexContainer>
          <FlexItem grow>
            <FormattedMessage id="credits.numberOfCredits" />
          </FlexItem>
          <FlexItem>
            <FormattedNumber value={quantity} />
          </FlexItem>
        </FlexContainer>
        <FlexContainer>
          <FlexItem grow>
            <FormattedMessage id="credits.pricePerCredit" />
          </FlexItem>
          <FlexItem>
            <FormattedNumber
              value={getPrice(quantity)}
              style="currency"
              currency="USD"
              minimumFractionDigits={2}
              maximumFractionDigits={2}
            />
          </FlexItem>
        </FlexContainer>
        <Text color="blue" bold size="lg">
          <FlexContainer>
            <FlexItem grow>
              <FormattedMessage id="credits.totalPrice" />
            </FlexItem>
            <FlexItem>
              <FormattedMessage
                id="credits.pricePlusTaxes"
                values={{
                  price: formatNumber(getPrice(quantity) * quantity, {
                    style: "currency",
                    currency: "USD",
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                  }),
                }}
              />
            </FlexItem>
          </FlexContainer>
        </Text>
      </FlexContainer>
    </Text>
  );
};

interface DiscountMessageProps {
  minimum: number;
  maximum?: number;
  discount: number;
}

const DiscountMessage: React.FC<DiscountMessageProps> = ({ maximum, minimum, discount }) => {
  const { formatNumber } = useIntl();
  const { isValid } = useFormState();
  const quantity = useWatch<CreditsFormValues>({ name: "quantity" });
  const isActive = quantity >= minimum && isValid;
  return (
    <FlexContainer gap="sm" alignItems="center">
      <Icon
        type={isActive ? "successOutline" : "infoOutline"}
        color={isActive && (!maximum || quantity < maximum) ? "success" : "disabled"}
      />
      <Text size="sm" color={isActive && (!maximum || quantity < maximum) ? "green600" : "grey600"}>
        <FormattedMessage
          id="credits.unlockDiscount"
          values={{ discount: formatNumber(discount, { style: "percent" }), minimum }}
        />
      </Text>
    </FlexContainer>
  );
};

const TalkToSalesBanner: React.FC = () => {
  const quantity = useWatch<CreditsFormValues>({ name: "quantity" });

  if (quantity <= MAX_CREDITS) {
    return null;
  }

  return (
    <Box mt="md">
      <Message
        type="info"
        text={
          <FormattedMessage
            id="credits.aboveMaxCredits"
            values={{
              lnk: (node: React.ReactNode) => (
                <Link to={links.contactSales} variant="primary" opensInNewTab>
                  {node}
                </Link>
              ),
            }}
          />
        }
      />
    </Box>
  );
};

export const CheckoutCreditsModal: React.FC<ModalContentProps<void>> = ({ onCancel }) => {
  const { formatMessage } = useIntl();
  const { mutateAsync: createCheckout } = useStripeCheckout();
  const workspaceId = useCurrentWorkspaceId();
  const analytics = useAnalyticsService();

  const startStripeCheckout = async (values: CreditsFormValues) => {
    // Use the current URL as a success URL but attach the STRIPE_SUCCESS_QUERY to it
    const successUrl = new URL(window.location.href);
    successUrl.searchParams.set(STRIPE_SUCCESS_QUERY, "true");
    const { stripeUrl } = await createCheckout({
      workspaceId,
      successUrl: successUrl.href,
      cancelUrl: window.location.href,
      stripeMode: "payment",
      quantity: values.quantity,
    });
    analytics.track(Namespace.CREDITS, Action.CHECKOUT_START, {
      actionDescription: "Checkout Start",
    });
    // Forward to stripe as soon as we created a checkout session successfully
    window.location.assign(stripeUrl);
  };

  const creditsFormSchema: SchemaOf<CreditsFormValues> = yup.object({
    quantity: yup
      .number()
      .integer(formatMessage({ id: "credits.noFractionalCredits" }))
      .transform((val) => (typeof val !== "number" || isNaN(val) ? 0 : val))
      .min(MIN_CREDITS, formatMessage({ id: "credits.minCreditsError" }, { minimum: MIN_CREDITS }))
      .max(MAX_CREDITS, formatMessage({ id: "credits.maxCreditsError" }, { maximum: MAX_CREDITS }))
      .required(),
  });

  return (
    <Form<CreditsFormValues>
      defaultValues={{ quantity: DEFAULT_CREDITS }}
      schema={creditsFormSchema}
      onSubmit={startStripeCheckout}
    >
      <ModalBody>
        <FormControl fieldType="input" type="number" name="quantity" />
        <PricePreview />
        <Box mt="lg">
          <FlexContainer direction="column">
            <DiscountMessage discount={0.07} minimum={2200} maximum={4800} />
            <DiscountMessage discount={0.17} minimum={4800} />
          </FlexContainer>
        </Box>
        <TalkToSalesBanner />
      </ModalBody>
      <ModalFooter>
        <ModalFormSubmissionButtons submitKey="credits.checkout" onCancelClickCallback={onCancel} />
      </ModalFooter>
    </Form>
  );
};
