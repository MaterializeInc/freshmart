const emptyStat = () => ({ max: 0, avg: 0, p99: 0 });

export const createInitialStats = () => ({
  view: emptyStat(),
  materializeView: emptyStat(),
  materialize: emptyStat(),
  mvRefresh: emptyStat(),
  viewEndToEnd: emptyStat(),
  materializeViewEndToEnd: emptyStat(),
  materializeEndToEnd: emptyStat(),
});

export const calculateMedian = (numbers) => {
  if (!Array.isArray(numbers) || numbers.length === 0) {
    return 0;
  }

  const sorted = [...numbers].sort((a, b) => a - b);
  const { length } = sorted;

  if (length % 2 === 0) {
    return (sorted[length / 2 - 1] + sorted[length / 2]) / 2;
  }

  return sorted[Math.floor(length / 2)];
};
