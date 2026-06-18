# ScheMatiQ bug fixes — apply locally and open PRs

Three independent fixes, each on its own branch / patch. Apply and review them
one at a time.

## Branch 1 — Resume after observation unit review (the error you screenshotted)
File: `1-fix-resume-observation-unit-review.patch`
Touches: `backend/app/api/routes/schematiq.py`
Fix: `/schematiq/resume` scheduled `schematiq_runner.resume_schematiq`, which
does not exist (the method is `resume_qbsd`). The endpoint returned 200 while the
background task crashed, so the pipeline never continued to schema discovery.

## Branch 2 — Monitor logs disappear when switching tabs
File: `2-fix-monitor-logs-persist-tab-switch.patch`
Touches: `frontend/src/pages/Visualize.tsx`
Fix: the Monitor's logs live in component-local state; Radix unmounts inactive
tabs by default. Added `forceMount` (+ `data-[state=inactive]:hidden`) so the
component stays mounted and keeps its logs.

## Branch 3 — Re-Extract progress bar stuck / no clear completion
Files: `backend/app/services/reextraction_service.py`,
       `frontend/src/components/DataTable/ExtractionProgressBar.tsx`
File: `3-fix-reextraction-progress-bar.patch`
Fix: progress was driven by documents *started*, so single-doc runs jumped to
100% then sat spinning, and multi-doc runs ran a step ahead. Now reports
completed documents and gives half-credit to the in-flight doc.

---

## How to apply each (run from the repo root, on a clean `main`)

git checkout main && git pull

# Branch 1
git checkout -b fix/resume-observation-unit-review
git am < 1-fix-resume-observation-unit-review.patch
git push -u origin fix/resume-observation-unit-review

# Branch 2
git checkout main
git checkout -b fix/monitor-logs-persist-tab-switch
git am < 2-fix-monitor-logs-persist-tab-switch.patch
git push -u origin fix/monitor-logs-persist-tab-switch

# Branch 3
git checkout main
git checkout -b fix/reextraction-progress-bar
git am < 3-fix-reextraction-progress-bar.patch
git push -u origin fix/reextraction-progress-bar

## Open the PRs (GitHub CLI)
gh pr create --base main --head fix/resume-observation-unit-review \
  --title "fix: resume after observation unit review calls correct runner method"
gh pr create --base main --head fix/monitor-logs-persist-tab-switch \
  --title "fix: preserve ScheMatiQ Monitor logs when switching tabs"
gh pr create --base main --head fix/reextraction-progress-bar \
  --title "fix: re-extraction progress bar advances honestly and never sticks at 100%"
