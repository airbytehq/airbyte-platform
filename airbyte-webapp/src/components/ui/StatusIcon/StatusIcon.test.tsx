import { render } from "@testing-library/react";

import { StatusIcon, StatusIconStatus } from "./StatusIcon";

describe("<StatusIcon />", () => {
  it("renders with title and default icon", () => {
    const title = "Pulpo";
    const value = 888;

    const component = render(<StatusIcon title={title} value={value} />);

    expect(component.getByTitle(title)).toBeDefined();
    expect(component.getByTestId("mocksvg")).toHaveAttribute("data-icon", "status-error");
    expect(component.getByText(`${value}`)).toBeDefined();
  });

  const statusCases: Array<{ status: StatusIconStatus; icon: string }> = [
    { status: "success", icon: "status-success" },
    { status: "inactive", icon: "status-inactive" },
    { status: "sleep", icon: "status-sleep" },
    { status: "warning", icon: "status-warning" },
    { status: "loading", icon: "circle-loader" },
    { status: "error", icon: "status-error" },
    { status: "cancelled", icon: "status-cancelled" },
  ];

  it.each(statusCases)("renders $status status", ({ status, icon }) => {
    const title = `Status is ${status}`;
    const value = 888;
    const props = {
      title,
      value,
      status,
    };

    const component = render(<StatusIcon {...props} />);

    expect(component.getByTitle(title)).toBeDefined();
    let element;
    if (status === "loading") {
      // renders <Icon />
      element = component.getByRole("img");
    } else {
      // renders <Icon />
      element = component.getByTestId("mocksvg");
    }
    expect(element).toHaveAttribute("data-icon", icon);
    expect(component.getByText(`${value}`)).toBeDefined();
  });
});
