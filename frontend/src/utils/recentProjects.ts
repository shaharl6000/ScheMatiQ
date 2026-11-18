// Per-browser registry of projects opened in the workspace.
//
// ScheMatiQ has no user accounts, so the server cannot tell which projects
// belong to whom, and it has no endpoint that lists sessions. The "recent
// projects" list is therefore kept in this browser. This is not access
// control: the API has no authentication, and anyone who knows a session id
// can read, edit and delete that project.

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
