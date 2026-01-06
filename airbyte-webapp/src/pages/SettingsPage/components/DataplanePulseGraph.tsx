import { useEffect, useRef, useState } from "react";
import { useIntl } from "react-intl";

import styles from "./DataplanePulseGraph.module.scss";

interface HeartbeatRecord {
  timestamp: number;
}

interface DataplanePulseGraphProps {
  heartbeats: HeartbeatRecord[];
}

export const DataplanePulseGraph: React.FC<DataplanePulseGraphProps> = ({ heartbeats }) => {
  const { formatMessage } = useIntl();
  const width = 50;
  const height = 30;
  const baselineY = height - 5;
  const [now, setNow] = useState(Date.now());
  const svgRef = useRef<SVGSVGElement>(null);

  // Update "now" every 50ms for sliding window animation
  useEffect(() => {
    const interval = setInterval(() => {
      setNow(Date.now());
    }, 50);
    return () => clearInterval(interval);
  }, []);

  if (heartbeats.length === 0) {
    return (
      <svg width={width} height={height} className={styles.pulseGraph}>
        <text x={width / 2} y={height / 2} textAnchor="middle" fill="var(--color-grey-400)" fontSize="10">
          {formatMessage({ id: "dataplaneHealth.noData" })}
        </text>
      </svg>
    );
  }

  // 5-minute window to show more activity history
  const windowMs = 5 * 60 * 1000;
  const windowStart = now - windowMs;

  // Filter heartbeats within the time window
  const recentHeartbeats = heartbeats
    .map((hb) => ({
      ...hb,
      timestampMs: new Date(hb.timestamp).getTime(),
    }))
    .filter((hb) => hb.timestampMs >= windowStart && hb.timestampMs <= now)
    .sort((a, b) => a.timestampMs - b.timestampMs);

  // Map timestamps to X positions (sliding window)
  const timeToX = (timestampMs: number) => {
    const relativeTime = timestampMs - windowStart;
    return (relativeTime / windowMs) * width;
  };

  return (
    <svg ref={svgRef} width={width} height={height} className={styles.pulseGraph}>
      {/* Heartbeat spikes */}
      {recentHeartbeats.map((hb, index) => {
        const x = timeToX(hb.timestampMs);
        const spikeHeight = 20;
        const baseColor = "var(--color-green-400)";

        const age = now - hb.timestampMs;
        const opacity = Math.max(0.2, 1 - (age / windowMs) * 0.8);

        return (
          <line
            key={`${hb.timestampMs}-${index}`}
            x1={x}
            y1={baselineY}
            x2={x}
            y2={baselineY - spikeHeight}
            stroke={baseColor}
            strokeWidth="2"
            strokeLinecap="round"
            opacity={opacity}
          />
        );
      })}
    </svg>
  );
};
