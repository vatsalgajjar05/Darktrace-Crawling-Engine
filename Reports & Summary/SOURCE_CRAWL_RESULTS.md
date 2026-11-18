# Source-by-Source Crawl Results

Date: 2026-11-18

Source registry: `sources.yaml`

| Source | Enabled | Queue | URL | Direct HTTP | Docker job ID | Docker result | Retry count |
|---|---|---|---|---|---|---|---|
| `news_source_1` | true | standard | `https://pypi.org/` | 200 | `87efaf34-07e3-46d1-a8c5-8880ce10fef7` | success, HTTP 200 | 0 post-fix |
| `paste_source_1` | true | fast | `https://pypi.org/search/?q=example` | 200 | `d238a1d0-2937-4609-b060-c3f51eb539e3` | success, HTTP 200 | 0 post-fix |
| `marketplace_source_1` | true | bulk | `https://pypi.org/` | 200 | `fbb3bf33-9806-4b2e-af74-0bb61c09ed6a` | success, HTTP 200 | 0 post-fix |
| `onion_source_1` | false | bulk | placeholder `.onion` address | NOT TESTED | NOT TESTED | excluded | 0 |

The direct verification result was `3/3 sources OK`. All live Docker jobs were received on the worker, completed on their configured queues, and updated Redis state.

A repeated `news_source_1` crawl returned HTTP 304, `unchanged=true`, and preserved the prior content hash.
