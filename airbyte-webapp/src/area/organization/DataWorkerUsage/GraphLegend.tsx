import { LegendProps } from "recharts";

import { Text } from "components/ui/Text";

import styles from "./GraphLegend.module.scss";

export const GraphLegend = (props: LegendProps) => {
  return (
    <ul className={styles.graphLegend}>
      {props.payload?.map((entry, index) => {
        if (!entry.payload) {
          return null;
        }
        const fill = entry.payload && "fill" in entry.payload ? (entry.payload.fill as string) : "#000";
        return <WorkspaceLegendItem key={`legend-item-${index}`} name={entry.value || ""} color={fill} />;
      })}
    </ul>
  );
};

interface WorkspaceLegendItemProps {
  name: string;
  color: string;
  usage?: number;
}

export const WorkspaceLegendItem = ({ name, color, usage }: WorkspaceLegendItemProps) => {
  return (
    <li className={styles.graphLegend__item}>
      <ColoredDot color={color} />
      <Text>{name}</Text>
      {usage && <Text color="grey">{usage}</Text>}
    </li>
  );
};

const ColoredDot = ({ color }: { color: string }) => {
  return (
    <span
      className={styles.graphLegend__dot}
      style={{
        backgroundColor: color,
      }}
    />
  );
};
