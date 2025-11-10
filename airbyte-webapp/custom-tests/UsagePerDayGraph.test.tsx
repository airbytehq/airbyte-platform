import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { UsagePerDayGraph } from "../src/packages/cloud/area/billing/components/UsagePerDayGraph/UsagePerDayGraph";

// Mocks de dependencias
jest.mock("react-intl", () => ({
  FormattedMessage: ({ id }: any) => <span data-testid="formatted-msg">{id}</span>,
  useIntl: () => ({ formatMessage: ({ id }: any) => id }),
}));

jest.mock("core/utils/numberHelper", () => ({
  useFormatCredits: () => ({ formatCredits: (v: number) => `${v.toFixed(2)} credits` }),
}));

jest.mock("components/ui/Text", () => ({
  Text: ({ children, ...props }: any) => <div data-testid="text" {...props}>{children}</div>,
}));

jest.mock("../src/packages/cloud/area/billing/components/FormattedCredits", () => ({
  FormattedCredits: ({ credits }: any) => <span data-testid="formatted-credits">{credits}</span>,
}));

jest.mock("classnames", () => (...args: any[]) => args.filter(Boolean).join(" "));

// Mock recharts
jest.mock("recharts", () => ({
  ResponsiveContainer: ({ children }: any) => <div data-testid="responsive">{children}</div>,
  BarChart: ({ children }: any) => <div data-testid="barchart">{children}</div>,
  Bar: ({ children, dataKey }: any) => <div data-testid={`bar-${dataKey}`}>{children}</div>,
  CartesianGrid: () => <div data-testid="cartesian" />,
  Legend: () => <div data-testid="legend" />,
  ReferenceLine: () => <div data-testid="reference-line" />,
  Tooltip: () => <div data-testid="tooltip" />,
  XAxis: () => <div data-testid="xaxis" />,
  YAxis: () => <div data-testid="yaxis" />,
  Cell: () => <div data-testid="cell" />,
}));

jest.mock("../src/packages/cloud/area/billing/components/UsagePerDayGraph/UsagePerDayGraph.module.scss", () => ({
  container: "container",
  "container--full": "container-full",
  "container--minimized": "container-minimized",
  grey100: "grey100",
  grey: "grey",
  darkBlue: "darkBlue",
  blue400: "blue400",
  green: "green",
  white: "white",
  tooltipWrapper: "tooltip-wrapper",
}));

describe("UsagePerDayGraph", () => {
  const chartData = [
    { timeChunkLabel: "Day 1", freeUsage: 10, billedCost: 20, internalUsage: 5 },
    { timeChunkLabel: "Day 2", freeUsage: 15, billedCost: 10, internalUsage: 3 },
  ];

  it("renderiza correctamente con datos básicos", () => {
    render(<UsagePerDayGraph chartData={chartData} />);

    expect(screen.getByTestId("responsive")).toBeInTheDocument();
    expect(screen.getByTestId("barchart")).toBeInTheDocument();
    expect(screen.getByTestId("bar-billedCost")).toBeInTheDocument();
    expect(screen.getByTestId("xaxis")).toBeInTheDocument();
    expect(screen.getByTestId("yaxis")).toBeInTheDocument();
    expect(screen.getByTestId("tooltip")).toBeInTheDocument();
    expect(screen.getByTestId("legend")).toBeInTheDocument();
  });

  it("agrega barras adicionales si tiene freeUsage e internalUsage", () => {
    render(<UsagePerDayGraph chartData={chartData} hasFreeUsage hasInternalUsage />);
    expect(screen.getByTestId("bar-freeUsage")).toBeInTheDocument();
    expect(screen.getByTestId("bar-internalUsage")).toBeInTheDocument();
  });

  it("usa el modo minimizado y oculta el legend y tooltip", () => {
    render(<UsagePerDayGraph chartData={chartData} minimized />);
    expect(screen.getByTestId("reference-line")).toBeInTheDocument();
    expect(screen.queryByTestId("legend")).not.toBeInTheDocument();
    expect(screen.queryByTestId("tooltip")).not.toBeInTheDocument();
  });

  it("maneja un array vacío sin fallar", () => {
    render(<UsagePerDayGraph chartData={[]} />);
    expect(screen.getByTestId("responsive")).toBeInTheDocument();
  });
});
