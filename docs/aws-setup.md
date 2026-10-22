# AWS setup checklist (do this yourself before Week 3)

Claude will write the Terraform but will not enter credentials or apply changes without your go-ahead.

1. Create a dedicated IAM user or SSO profile for this project (not the root account).
2. Run `aws configure --profile tidemark` (or `aws configure sso`).
3. **Billing → Budgets → Create budget:** monthly cost budget, e.g. $30, with email alerts at 50%, 80% and 100% (actual) and 100% (forecasted).
4. Optionally enable Cost Anomaly Detection.
5. Tell Claude which profile name and region to use.
