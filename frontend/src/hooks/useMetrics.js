import { useEffect, useMemo, useRef, useState } from 'react';
import {
  METRICS_MAX_RETRIES,
  METRICS_POLL_INTERVAL,
  HISTORY_WINDOW_MS,
} from '../constants/config.js';
import { getMetrics } from '../services/api.js';
import { createInitialStats } from '../utils/statistics.js';

const shouldRetry = (error) =>
  error?.code === 'ECONNABORTED' || error?.code === 'ECONNREFUSED';

export const useMetrics = ({ productId, onError }) => {
  const [metrics, setMetrics] = useState([]);
  const [stats, setStats] = useState(() => createInitialStats());
  const [isFetching, setIsFetching] = useState(false);

  const retryCountRef = useRef(0);
  const isActiveRef = useRef(true);
  const timeoutRef = useRef(null);

  useEffect(() => {
    isActiveRef.current = true;
    retryCountRef.current = 0;
    setMetrics([]);
    setStats(createInitialStats());

    const pollMetrics = async () => {
      if (!isActiveRef.current) {
        return;
      }

      try {
        setIsFetching(true);
        const response = await getMetrics(productId);
        const data = response.data;

        if (!data) {
          return;
        }

        retryCountRef.current = 0;
        onError?.(null);

        const now = Date.now();
        const timestamp = data.timestamp;

        setMetrics((prev) => {
          const filtered = prev.filter((entry) => now - entry.timestamp <= HISTORY_WINDOW_MS);
          const lastMetric = filtered[filtered.length - 1] || {};

          const newMetric = {
            timestamp,
            isolation_level: data.isolation_level,
            view_latency: data.view_latency,
            view_end_to_end_latency: data.view_end_to_end_latency,
            view_price: data.view_price ?? lastMetric.view_price,
            view_qps: data.view_qps,
            view_stats: data.view_stats,
            view_end_to_end_stats: data.view_end_to_end_stats,
            materialized_view_latency: data.materialized_view_latency,
            materialized_view_end_to_end_latency: data.materialized_view_end_to_end_latency,
            materialized_view_price:
              data.materialized_view_price ?? lastMetric.materialized_view_price,
            materialized_view_qps: data.materialized_view_qps,
            materialized_view_freshness: data.materialized_view_freshness,
            materialized_view_refresh_duration: data.materialized_view_refresh_duration,
            materialized_view_stats: data.materialized_view_stats,
            materialized_view_end_to_end_stats: data.materialized_view_end_to_end_stats,
            materialized_view_refresh_stats: data.materialized_view_refresh_stats,
            materialize_latency: data.materialize_latency,
            materialize_end_to_end_latency: data.materialize_end_to_end_latency,
            materialize_price: data.materialize_price ?? lastMetric.materialize_price,
            materialize_qps: data.materialize_qps,
            materialize_freshness: data.materialize_freshness,
            materialize_stats: data.materialize_stats,
            materialize_end_to_end_stats: data.materialize_end_to_end_stats,
          };

          return [...filtered, newMetric];
        });

        setStats((prev) => ({
          ...prev,
          view: data.view_stats
            ? {
                max: data.view_stats.max,
                avg: data.view_stats.average,
                p99: data.view_stats.p99,
              }
            : prev.view,
          viewEndToEnd: data.view_end_to_end_stats
            ? {
                max: data.view_end_to_end_stats.max,
                avg: data.view_end_to_end_stats.average,
                p99: data.view_end_to_end_stats.p99,
              }
            : prev.viewEndToEnd,
          materializeView: data.materialized_view_stats
            ? {
                max: data.materialized_view_stats.max,
                avg: data.materialized_view_stats.average,
                p99: data.materialized_view_stats.p99,
              }
            : prev.materializeView,
          materializeViewEndToEnd: data.materialized_view_end_to_end_stats
            ? {
                max: data.materialized_view_end_to_end_stats.max,
                avg: data.materialized_view_end_to_end_stats.average,
                p99: data.materialized_view_end_to_end_stats.p99,
              }
            : prev.materializeViewEndToEnd,
          materialize: data.materialize_stats
            ? {
                max: data.materialize_stats.max,
                avg: data.materialize_stats.average,
                p99: data.materialize_stats.p99,
              }
            : prev.materialize,
          materializeEndToEnd: data.materialize_end_to_end_stats
            ? {
                max: data.materialize_end_to_end_stats.max,
                avg: data.materialize_end_to_end_stats.average,
                p99: data.materialize_end_to_end_stats.p99,
              }
            : prev.materializeEndToEnd,
          mvRefresh: data.materialized_view_refresh_stats
            ? {
                max: data.materialized_view_refresh_stats.max * 1000,
                avg: data.materialized_view_refresh_stats.average * 1000,
                p99: data.materialized_view_refresh_stats.p99 * 1000,
              }
            : prev.mvRefresh,
        }));
      } catch (error) {
        const friendlyError = error?.response?.data?.detail || error?.message;
        onError?.(friendlyError);

        if (shouldRetry(error)) {
          retryCountRef.current += 1;
          if (retryCountRef.current <= METRICS_MAX_RETRIES) {
            const delay = 1000 * retryCountRef.current;
            timeoutRef.current = setTimeout(pollMetrics, delay);
            return;
          }
        }
      } finally {
        setIsFetching(false);
      }

      if (isActiveRef.current) {
        timeoutRef.current = setTimeout(pollMetrics, METRICS_POLL_INTERVAL);
      }
    };

    pollMetrics();

    return () => {
      isActiveRef.current = false;
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, [productId, onError]);

  const currentMetric = useMemo(
    () => (metrics.length > 0 ? metrics[metrics.length - 1] : {}),
    [metrics],
  );

  return {
    metrics,
    stats,
    isFetching,
    currentMetric,
  };
};
