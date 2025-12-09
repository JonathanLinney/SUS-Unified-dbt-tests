# sus_unified_dbt_project

Minimal README for the dbt project.

Quick start:
1. Create a GitHub repo and note the remote URL.
2. Run:
   - git init
   - git add .
   - git commit -m "Initial commit"
   - git remote add origin <your-remote-url>
   - git branch -M main
   - git push -u origin main

CI:
- A GitHub Actions workflow runs dbt compile on pushes to main (requires repo secrets for dbt/Snowflake credentials if you extend it).
