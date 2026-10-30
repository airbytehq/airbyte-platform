import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useGetPaymentInformation } from "core/api";
import { CustomerPortalRequestBodyFlow } from "core/api/types/AirbyteClient";

import { PaymentMethod } from "./PaymentMethod";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

const mockGoToCustomerPortalByFlow: Partial<Record<CustomerPortalRequestBodyFlow, jest.Mock>> = {
  portal: jest.fn(),
  payment_method: jest.fn(),
};

jest.mock("cloud/area/billing/utils/useRedirectToCustomerPortal", () => ({
  useRedirectToCustomerPortal: (flow: CustomerPortalRequestBodyFlow) => ({
    goToCustomerPortal: mockGoToCustomerPortalByFlow[flow],
    redirecting: false,
  }),
}));

jest.mock("core/api", () => ({
  useGetPaymentInformation: jest.fn(),
}));

const mockPaymentInformation = (hasPaymentMethod: boolean) => {
  mocked(useGetPaymentInformation).mockReturnValue({
    data: {
      customer: {
        defaultPaymentMethod: hasPaymentMethod
          ? { type: "card", cardBrand: "visa", cardLastDigits: "4242", cardExpireMonth: 1, cardExpireYear: 2030 }
          : undefined,
      },
    },
    isLoading: false,
    isError: false,
  } as unknown as ReturnType<typeof useGetPaymentInformation>);
};

describe("PaymentMethod", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("with a payment method", () => {
    beforeEach(() => mockPaymentInformation(true));

    it("should open the full customer portal from the update link", async () => {
      const wrapper = await render(<PaymentMethod />);

      await userEvent.click(wrapper.getByRole("button"));

      expect(mockGoToCustomerPortalByFlow.portal).toHaveBeenCalledTimes(1);
    });
  });

  describe("without a payment method", () => {
    beforeEach(() => mockPaymentInformation(false));

    it("should open the payment method flow from the add button", async () => {
      const wrapper = await render(<PaymentMethod />);

      await userEvent.click(wrapper.getByRole("button"));

      expect(mockGoToCustomerPortalByFlow.payment_method).toHaveBeenCalledTimes(1);
    });

    it("should show only the add button", async () => {
      const wrapper = await render(<PaymentMethod />);

      expect(wrapper.getAllByRole("button")).toHaveLength(1);
    });
  });
});
