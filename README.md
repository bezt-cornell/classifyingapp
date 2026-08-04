# ClassifyingApp

## Local development with Docker Compose

Local development uses environment variables from `.env`.

### Start the app locally

```bash
docker compose up --build
```

### Local Docker build

The local compose setup builds with `local.Dockerfile`.

```bash
docker build -f local.Dockerfile -t classifyingapp:local .
```

### Production Docker build

Use the production Dockerfile for AWS deployment:

```bash
docker build -f production.Dockerfile -t classifyingapp:prod .
```

### Local `.env` example

Create a `.env` file in the project root with values like:

```env
SECRET_KEY=change-me
client_id=your-google-client-id
client_secret=your-google-client-secret
token_uri=https://oauth2.googleapis.com/token
auth_uri=https://accounts.google.com/o/oauth2/auth
```

### Production with AWS Secrets Manager

Set the following production environment variables:

- `USE_AWS_SECRETS_MANAGER=true`
- `AWS_SECRETS_NAME=my-app-secret`
- `AWS_REGION=us-east-1`

Then store secrets in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name my-app-secret \
  --secret-string '{"SECRET_KEY":"prod-secret","client_id":"your-google-client-id","client_secret":"your-google-client-secret"}'
```

### Notes

- Local development uses `.env` values.
- Production uses AWS Secrets Manager only when `USE_AWS_SECRETS_MANAGER=true`.
- In production, use IAM/task roles for AWS credentials.
