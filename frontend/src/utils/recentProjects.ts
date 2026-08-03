// Per-browser registry of projects opened in the workspace.
//
// ScheMatiQ has no server-side user scoping: the /sessions endpoints return
// every session for every user. To avoid surfacing other people's projects in
// the "recent projects" list, we track ids locally in this browser instead of
// listing everything from the server. This is a UI-scoping measure, not access
// control — the API itself remains open, and a known session id still resolves.

const STORAGE_KEY = 'workspace.recentProjectIds';
const MAX_STORED = 30;

export function getRecentProjectIds(): string[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((id): id is string => typeof id === 'string');
  } catch {
    return [];
  }
}

export function rememberProject(sessionId: string): void {
  if (!sessionId) return;
  try {
    const ids = getRecentProjectIds().filter((id) => id !== sessionId);
    ids.unshift(sessionId);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(ids.slice(0, MAX_STORED)));
  } catch {
    // localStorage unavailable (private mode / quota) — the recent list simply
    // stays empty, which is acceptable.
  }
}

export function forgetProjects(sessionIds: string[]): void {
  if (sessionIds.length === 0) return;
  try {
    const drop = new Set(sessionIds);
    const ids = getRecentProjectIds().filter((id) => !drop.has(id));
    localStorage.setItem(STORAGE_KEY, JSON.stringify(ids));
  } catch {
    // ignore
  }
}
