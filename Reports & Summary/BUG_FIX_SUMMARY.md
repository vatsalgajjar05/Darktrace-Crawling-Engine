# Bug-Fix Summary

## BUG-20261118-01

Status: FIXED and verified.

### Reproduction

Run a fresh configured crawl that leaves more than the five-page spider bound in the durable pending frontier. The first live PyPI jobs for `news_source_1` and `marketplace_source_1` returned pending checkpoint counts and entered Celery retries/dead-letter handling despite successful seed/content processing.

### Root cause

`engine/runner.py` rejected every successful checkpoint run with any pending URL. It did not distinguish a fresh bounded crawl from an interrupted recovery that was still supposed to drain its saved frontier.

### Fix

Pending URLs are rejected only when `resume_progress=True`. Fresh bounded crawls may complete with a remaining discovery frontier.

### Affected files

- `engine/runner.py`
- `tests/test_week8.py`

### Regression test

`test_fresh_crawl_accepts_bounded_pending_frontier` verifies that a fresh crawl with a pending frontier succeeds while the existing `test_runner_rejects_success_with_pending_checkpoint_urls` verifies recovery runs still reject incomplete frontiers.

### Before/after

- Before: 24 live attempts could lead to Celery retries/dead-letter for the two bounded PyPI sources.
- After: all three enabled Docker Celery jobs succeeded with HTTP 200 and 0 post-fix retries.
