# Class 23 — Git, GitHub Actions and CI/CD

## Objectives

* Use git for the daily loop: branch, commit, push, pull request, review, merge.
* Read `.github/workflows/validate.yml` and explain each job and step.
* Configure repository secrets and a GitHub environment with approval.
* Explain why deploys happen from CI, not laptops.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–25 | Mini-lesson: git (init, status, add, commit, log, branch, merge, push) |
| 25–45 | Pull requests and code review on the class repo |
| 45–70 | The workflow file: lint → tests → data dictionary → bundle validate → deploy |
| 70–85 | Secrets and environments; the prod approval gate |
| 85–95 | Ruff: what a linter catches; fix three findings live |
| 95–100 | Homework |

## Git (25 min)

Hands-on with the project repo:

```bash
git status
git checkout -b feature/customer-comments
# edit gold.json
git add src/config/gold.json
git commit -m "gold: add column comments for dim_customer"
git push -u origin feature/customer-comments
```

Concepts: a commit is a snapshot with a message; a branch is a line of commits; `main`
is what is deployed. Show `git log --oneline` and `git diff main`.

## Pull requests (20 min)

Open the PR on GitHub. Reviewer checklist for this project (put it in the PR template):
config change has a comment for every new column; PII columns declared; tests pass;
data dictionary regenerated. Merge; see CI run on `main`.

## The workflow (25 min)

Read `.github/workflows/validate.yml`:

* `on: pull_request` and `push: main` — every PR is checked, `main` deploys.
* job `lint-and-unit-tests`: checkout, Python 3.11, Java 17 (for local Spark), `pip install -r requirements-dev.txt`, `ruff check .`, `pytest`, then `python -m common_utils.gold` + `git diff --exit-code docs/data-dictionary.md` — the docs must match the config.
* job `bundle-validate`: `databricks/setup-cli` + `databricks bundle validate -t dev` with host/token from secrets.
* job `deploy-dev`: only on `main`, `environment: dev` (approval and secrets scoped to the environment), `bundle deploy -t dev`.

Draw the pipeline; ask where a prod deploy job would go (after dev, with `environment: prod`
requiring a reviewer, using a service-principal token).

## Secrets and environments (15 min)

GitHub → Settings → Secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`. Environments: `dev`
(auto), `prod` (required reviewers). Rule: the CI token is not a personal token in prod —
it belongs to the service principal from Class 21.

## Ruff (10 min)

`ruff check .` flags unused imports, bad `zip` without `strict`, unsorted imports.
Introduce three errors on purpose, read the messages, `ruff check --fix`. Linting is
"a reviewer that never gets tired" — `pyproject.toml` holds the rules and excludes
notebooks (Databricks magics).

## Homework

1. Open a PR that adds a quality rule; get it reviewed by a classmate; merge it and watch the deploy.
2. Make CI fail on purpose (break a test) and read the log to the failing line.
3. Write the `deploy-prod` job (do not merge it) using `environment: prod`.

## Common problems

* Committing `__pycache__` or `.databricks/` — `.gitignore` covers both; check `git status`.
* Data-dictionary step fails → run `python -m common_utils.gold` locally and commit the file.
* `bundle validate` in CI fails on auth, not on config — secrets not set on the environment.
