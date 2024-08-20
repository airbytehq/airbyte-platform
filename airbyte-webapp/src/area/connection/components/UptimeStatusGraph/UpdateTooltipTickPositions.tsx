import { CategoricalChartState } from "recharts/types/chart/generateCategoricalChart";

import { adjustPositions } from "../HistoricalOverview/ChartConfig";

export const UpdateTooltipTickPositions = ({ orderedTooltipTicks, xAxisMap }: CategoricalChartState) => {
  const xAxisDefinition = xAxisMap?.[0];
  if (orderedTooltipTicks && xAxisDefinition) {
    adjustPositions(orderedTooltipTicks, xAxisDefinition);
  }
  return null;
};
UpdateTooltipTickPositions.displayName = "Customized"; // must be this to be rendered, via https://github.com/recharts/recharts/blob/e4dd3710642428692e2ebda5a51c0174f3a6f361/src/chart/generateCategoricalChart.tsx#L2336
