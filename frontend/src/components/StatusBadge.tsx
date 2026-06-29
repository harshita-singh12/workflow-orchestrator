const COLORS: Record<string, string> = {
  PENDING: "neutral",
  READY: "neutral",
  QUEUED: "info",
  RUNNING: "info",
  RETRY_BACKOFF: "warning",
  WAITING: "warning",
  COMPLETED: "success",
  SUCCEEDED: "success",
  FAILED: "danger",
  EXPIRED: "danger",
  SKIPPED: "neutral",
  CANCELLED: "neutral",
  ABANDONED: "neutral",
  LEASED: "info",
};

export default function StatusBadge({ status }: { status: string }) {
  const tone = COLORS[status] ?? "neutral";
  return <span className={`badge badge-${tone}`}>{status}</span>;
}
