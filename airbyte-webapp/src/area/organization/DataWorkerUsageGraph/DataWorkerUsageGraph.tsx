import dayjs from "dayjs";
import { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  TooltipProps,
  XAxis,
  YAxis,
} from "recharts";

import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { RangeDatePicker } from "components/ui/DatePicker";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useOrganizationWorkerUsage } from "core/api";
import { RegionDataWorkerUsage } from "core/api/types/AirbyteClient";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";

import styles from "./DataWorkerUsageGraph.module.scss";
import { GraphLegend } from "./GraphLegend";
import { GraphTooltip } from "./GraphTooltip";

const now = new Date();

export const tooltipConfig: TooltipProps<number, string> = {
  animationDuration: 300,
  animationEasing: "ease-out",
  allowEscapeViewBox: { x: false, y: true },
  isAnimationActive: false,
};

export const DataWorkerUsageGraph: React.FC = () => {
  // RangeDatePicker fires onChange even if only one date is selected. We store that state here, and when both dates
  // have been selected, they get stored in selectedDateRange, which is what we use to fetch data.
  const [tempDateRange, setTempDateRange] = useState<[string, string]>([
    dayjs(now).subtract(30, "day").startOf("day").format("YYYY-MM-DD"),
    dayjs(now).endOf("day").format("YYYY-MM-DD"),
  ]);
  const [selectedDateRange, setSelectedDateRange] = useState<[string, string]>([
    dayjs(now).subtract(30, "day").startOf("day").format("YYYY-MM-DD"),
    dayjs(now).endOf("day").format("YYYY-MM-DD"),
  ]);

  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const usageByRegion = useOrganizationWorkerUsage({
    startDate: selectedDateRange[0],
    endDate: selectedDateRange[1],
  });
  const { formatMessage } = useIntl();

  const selectedUsage = useMemo(() => {
    if (selectedRegion === "all") {
      return undefined;
    }
    return usageByRegion?.regions.find((region) => region.id === selectedRegion);
  }, [selectedRegion, usageByRegion]);

  const regionOptions = useMemo(() => {
    return [...usageByRegion.regions.map((region) => ({ label: region.name, value: region.id }))];
  }, [usageByRegion]);

  return (
    <PageContainer>
      <FlexContainer direction="column" alignItems="stretch">
        <FlexContainer alignItems="center" justifyContent="space-between">
          <FlexItem>
            <ListBox
              options={regionOptions}
              onSelect={setSelectedRegion}
              selectedValue={selectedRegion}
              placeholder={formatMessage({ id: "settings.organization.usage.selectRegion" })}
            />
          </FlexItem>
          <FlexItem>
            <RangeDatePicker
              maxDate={new Date().toString()}
              value={tempDateRange}
              onChange={(dates) => setTempDateRange([dates[0], dates[1]])}
              onClose={() => {
                if (tempDateRange[0] !== "" && tempDateRange[1] !== "") {
                  setSelectedDateRange([tempDateRange[0], tempDateRange[1]]);
                  // TODO: once we're getting real data from the backend, we won't get random workspace IDs back, so we
                  // probably don't need to reset this.
                } else {
                  setTempDateRange(selectedDateRange);
                }
              }}
            />
          </FlexItem>
        </FlexContainer>
        {selectedRegion && (
          <Box mt="xl">
            <RegionUsage usage={selectedUsage} dateRange={selectedDateRange} />
          </Box>
        )}
      </FlexContainer>
    </PageContainer>
  );
};

interface RegionUsageProps {
  usage: RegionDataWorkerUsage | undefined;
  dateRange: [string, string];
}

interface RegionDataBar {
  date: dayjs.Dayjs;
  formattedDate: string;
  workspaceUsage: Record<string, number>;
}

interface ColorMap {
  gridLine: string;
  workspaceColor1: string;
  workspaceColor2: string;
  workspaceColor3: string;
  barHover: string;
}

const RegionUsage = ({ usage: regionDataWorkerUsage, dateRange }: RegionUsageProps) => {
  const { formatMessage } = useIntl();
  const [colorMap, setColorMap] = useState<ColorMap>({
    gridLine: "",
    workspaceColor1: "",
    workspaceColor2: "",
    workspaceColor3: "",
    barHover: "",
  });
  const { colorValues } = useAirbyteTheme();

  useEffect(() => {
    const colorMap: ColorMap = {
      gridLine: colorValues[styles.gridLine],
      workspaceColor1: colorValues[styles.workspaceColor1],
      workspaceColor2: colorValues[styles.workspaceColor2],
      workspaceColor3: colorValues[styles.workspaceColor3],
      barHover: colorValues[styles.barHover],
    };
    setColorMap(colorMap);
  }, [colorValues]);

  const { formatDate } = useIntl();

  const stackedWorkspaceSections: Array<{ dataKey: string; fill: string; name: string }> = useMemo(
    () =>
      regionDataWorkerUsage?.workspaces.map((workspaceUsage, index) => {
        const colorOptions = [colorMap.workspaceColor1, colorMap.workspaceColor2, colorMap.workspaceColor3];
        const color = colorOptions[index % colorOptions.length];
        return {
          dataKey: `workspaceUsage.${workspaceUsage.id}`,
          name: workspaceUsage.name,
          fill: color,
        };
      }) ?? [],
    [colorMap.workspaceColor1, colorMap.workspaceColor2, colorMap.workspaceColor3, regionDataWorkerUsage?.workspaces]
  );

  const data = useMemo(() => {
    const firstDay = dayjs(dateRange[0]).startOf("day");
    const lastDay = dayjs(dateRange[1]).endOf("day");
    let cursor = firstDay;
    const days: Map<string, RegionDataBar> = new Map();
    while (cursor.isBefore(lastDay)) {
      const year = cursor.year().toString();
      const month = (cursor.month() + 1).toString().padStart(2, "0");
      const day = cursor.date().toString().padStart(2, "0");
      days.set(`${year}-${month}-${day}`, {
        date: cursor,
        formattedDate: cursor.format("YYYY-MM-DD"),
        workspaceUsage: {},
      });
      cursor = cursor.add(1, "day");
    }

    if (regionDataWorkerUsage) {
      regionDataWorkerUsage.workspaces.forEach((workspace) => {
        workspace.dataWorkers.forEach(({ date, used }) => {
          const day = days.get(date);
          if (day) {
            const existingUsage = day.workspaceUsage[workspace.id] ?? 0;
            day.workspaceUsage[workspace.id] = existingUsage + used;
          }
        });
      });
    }

    return Array.from(days.values());
  }, [dateRange, regionDataWorkerUsage]);

  const graphHeight = useMemo(() => {
    const numberOfWorkspaces = regionDataWorkerUsage?.workspaces.length ?? 0;
    return 200 + numberOfWorkspaces * 18.89;
  }, [regionDataWorkerUsage?.workspaces.length]);

  const animationDuration = useMemo(() => {
    const numberOfWorkspaces = Math.max(1, regionDataWorkerUsage?.workspaces.length ?? 0);
    return 300 / numberOfWorkspaces;
  }, [regionDataWorkerUsage?.workspaces.length]);

  if (!regionDataWorkerUsage || regionDataWorkerUsage.workspaces.length === 0) {
    return (
      <Box>
        <Text italicized color="grey">
          <FormattedMessage id="settings.organization.usage.noData" values={{ name: regionDataWorkerUsage?.name }} />
        </Text>
      </Box>
    );
  }

  return (
    <FlexContainer direction="column" gap="lg">
      <Heading as="h2" size="sm">
        <FormattedMessage id="settings.organization.usageByWorkspace" />
      </Heading>
      <Box pt="md" className={styles.dataWorkerUsageGraph__wrapper}>
        <ResponsiveContainer width="99%" minHeight={graphHeight}>
          <BarChart
            data={data}
            margin={{
              top: 0,
              right: 0,
              left: 0,
              bottom: 0,
            }}
          >
            <Legend
              verticalAlign="bottom"
              iconType="circle"
              layout="vertical"
              wrapperStyle={{ left: 0 }}
              content={<GraphLegend />}
            />
            <XAxis
              tickFormatter={(value) => formatDate(value, { month: "short", day: "numeric" })}
              dataKey="formattedDate"
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 10 }}
            />
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 12 }}
              tickFormatter={(value) => formatMessage({ id: "settings.organization.usage.graph.yAxisTick" }, { value })}
              minTickGap={10}
              allowDecimals={false}
              tickMargin={10}
            />

            <Tooltip
              wrapperStyle={{ outline: "none", zIndex: styles.tooltipZindex }}
              content={GraphTooltip}
              cursor={{ fill: colorMap.barHover }}
              {...tooltipConfig}
            />
            <CartesianGrid stroke={colorMap.gridLine} vertical={false} />
            {stackedWorkspaceSections.map((day, index) => (
              <Bar
                stackId="a"
                key={day.dataKey}
                name={day.name}
                dataKey={day.dataKey}
                fill={day.fill}
                barSize={16}
                animationDuration={animationDuration}
                animationBegin={animationDuration * index}
                animationEasing="linear"
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </Box>
    </FlexContainer>
  );
};
