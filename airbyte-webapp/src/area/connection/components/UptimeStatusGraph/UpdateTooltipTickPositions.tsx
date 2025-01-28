import { CategoricalChartState } from "recharts/types/chart/generateCategoricalChart";

import { adjustPositions } from "../HistoricalOverview/ChartConfig";

export const UpdateTooltipTickPositions = ({ orderedTooltipTicks, xAxisMap }: CategoricalChartState) => {
  const xAxisDefinition = xAxisMap?.[0];
  if (orderedTooltipTicks && xAxisDefinition) {
    adjustPositions(orderedTooltipTicks, xAxisDefinition);
  }

  // targeting svg elements via class name has to happen from a `style` element inside the svg
  // so render it out here instead of through classNames in the graph components
  return (
    <defs>
      <style
        type="text/css"
        // eslint-disable-next-line react/no-danger
        dangerouslySetInnerHTML={{
          __html: `
            .recharts-cartesian-axis-ticks {
              g:not(:first-child):not(:last-child) {
                display: none;
              }
            }
          `,
        }}
      />
    </defs>
  );
};
UpdateTooltipTickPositions.displayName = "Customized"; // must be this to be rendered, via https://github.com/recharts/recharts/blob/e4dd3710642428692e2ebda5a51c0174f3a6f361/src/chart/generateCategoricalChart.tsx#L2336
