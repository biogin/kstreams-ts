// Reading metrics in tests, unit and integration alike.

import type { Metric } from '../../src/index.js'

/** The metrics named `name` whose labels include `labels`. */
export function withLabels(
  metrics: readonly Metric[],
  name: Metric['name'],
  labels: Record<string, unknown> = {},
): Metric[] {
  return metrics.filter(
    (metric) =>
      metric.name === name &&
      Object.entries(labels).every(
        ([label, value]) => (metric.labels as Record<string, unknown>)[label] === value,
      ),
  )
}

/** The sum of the counter `name`, over the metrics whose labels include `labels`. */
export function total(
  metrics: readonly Metric[],
  name: Metric['name'],
  labels: Record<string, unknown> = {},
): number {
  return withLabels(metrics, name, labels).reduce((sum, metric) => sum + metric.value, 0)
}
