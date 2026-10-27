#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f data/icbar.sqlite3.gz ]]; then
  echo '[checkpoint] No database snapshot found; nothing to commit.'
  exit 0
fi
if [[ ! -d .git ]]; then
  echo '[checkpoint] Not a git repository; saved files locally.'
  exit 0
fi

git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
git add -- data/icbar.sqlite3.gz data/photos.tsv data/summary.json download_photos.sh
if git diff --cached --quiet; then
  echo '[checkpoint] No changes.'
  exit 0
fi

git commit -m "data: ICBAR crawler checkpoint $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
# Concurrency prevents multiple crawler jobs on the same branch at once.
# Fail explicitly if a commit cannot be pushed; do not pretend it is durable.
for attempt in 1 2 3; do
  if git push origin "HEAD:${GITHUB_REF_NAME:-main}"; then
    echo '[checkpoint] Pushed successfully.'
    exit 0
  fi
  echo "[checkpoint] Push failed: retry $attempt/3" >&2
  sleep "$((attempt * 4))"
done
echo '[checkpoint] ERROR: not pushed, progress is NOT durable across runners.' >&2
exit 1
