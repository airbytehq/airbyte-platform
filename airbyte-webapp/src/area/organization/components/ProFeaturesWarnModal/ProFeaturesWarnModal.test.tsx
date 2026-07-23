import { render } from "test-utils";

import { ProFeaturesWarnModal } from "./ProFeaturesWarnModal";

describe("ProFeaturesWarnModal", () => {
  it("uses Plus or Pro copy for sub-hourly syncs", async () => {
    const wrapper = await render(<ProFeaturesWarnModal variant="upgrade" featureId="sub-hourly-sync" />);

    expect(wrapper.container).toHaveTextContent("Upgrade to experience Plus or Pro features");
    expect(wrapper.container).toHaveTextContent("15-minute sync frequency");
    expect(wrapper.container).toHaveTextContent("Upgrade to Plus or Pro to unlock 15-minute syncs.");
  });

  it("keeps generic Pro copy for other features", async () => {
    const wrapper = await render(<ProFeaturesWarnModal variant="upgrade" featureId="rbac" />);

    expect(wrapper.container).toHaveTextContent("Upgrade to experience Pro features");
    expect(wrapper.container).toHaveTextContent("Role-based access control");
    expect(wrapper.container).not.toHaveTextContent("Plus or Pro");
  });
});
