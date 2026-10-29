import { memo, useEffect, useRef, useState } from "react";
import { useIntl } from "react-intl";

import styles from "./DataplanePulseGraph.module.scss";

interface HeartbeatRecord {
  timestamp: number;
}

interface DataplanePulseGraphProps {
  heartbeats: HeartbeatRecord[];
}

// The graph is 50px wide and covers 5 minutes, so the heartbeats move one pixel every 6 seconds.
const UPDATE_INTERVAL_MS = 6000;

export const DataplanePulseGraph: React.FC<DataplanePulseGraphProps> = memo(({ heartbeats }) => {
  const { formatMessage } = useIntl();
  const width = 50;
  const height = 30;
  const baselineY = height - 5;
  const [now, setNow] = useState(Date.now());
  const svgRef = useRef<SVGSVGElement>(null);

  // Update "now" once per pixel of movement; the CSS transition on the spikes animates the slide in between
  useEffect(() => {
    const interval = setInterval(() => {
      setNow(Date.now());
    }, UPDATE_INTERVAL_MS);
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

  // Filter heartbeats within the time window. There is no upper bound: "now" only updates every UPDATE_INTERVAL_MS,
  // so a heartbeat that arrived since the last update is newer than "now" and must still show. The svg clips it at its edge.
  const recentHeartbeats = heartbeats
    .map((hb) => ({
      ...hb,
      timestampMs: new Date(hb.timestamp).getTime(),
    }))
    .filter((hb) => hb.timestampMs >= windowStart)
    .sort((a, b) => a.timestampMs - b.timestampMs);

  // Map timestamps to X positions (sliding window)
  const timeToX = (timestampMs: number) => {
    const relativeTime = timestampMs - windowStart;
    return (relativeTime / windowMs) * width;
  };

  return (
    <svg ref={svgRef} width={width} height={height} className={styles.pulseGraph}>
      {/* Heartbeat spikes */}
      {recentHeartbeats.map((hb) => {
        const x = timeToX(hb.timestampMs);
        const spikeHeight = 20;
        const baseColor = "var(--color-green-400)";

        const age = Math.max(0, now - hb.timestampMs);
        const opacity = Math.max(0.2, 1 - (age / windowMs) * 0.8);

        return (
          <line
            key={hb.timestampMs}
            x1={0}
            y1={baselineY}
            x2={0}
            y2={baselineY - spikeHeight}
            stroke={baseColor}
            strokeWidth="2"
            strokeLinecap="round"
            className={styles.pulseGraph__spike}
            style={{ transform: `translateX(${x}px)`, opacity, transitionDuration: `${UPDATE_INTERVAL_MS}ms` }}
          />
        );
      })}
    </svg>
  );
});

DataplanePulseGraph.displayName = "DataplanePulseGraph";
