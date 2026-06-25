import type { FlumeGitHubNotification } from "@/types"

export function flumeExtractGitHubMeta(
  notification: FlumeGitHubNotification,
): Record<string, string> {
  return {
    event_type: "notification",
    reason: notification.reason,
    subject_type: notification.subject.type,
    repository: notification.repository.full_name,
    thread_id: notification.id,
  }
}
