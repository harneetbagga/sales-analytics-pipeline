#!/usr/bin/env bash
# Create the SNS topic that carries pipeline alerts and subscribe email addresses to it.
#
#   REGION=us-east-1 scripts/setup_sns_alerts.sh you@example.com [other@example.com ...]
#
# Safe to re-run: create-topic returns the existing topic. Each address receives a
# confirmation email from AWS and gets alerts only after clicking Confirm.
# Needs sns:CreateTopic and sns:Subscribe.
set -euo pipefail

REGION="${REGION:-us-east-1}"
TOPIC_NAME="${TOPIC_NAME:-sales-analytics-alerts}"

if [[ $# -lt 1 ]]; then
  echo "usage: REGION=<region> $0 <email> [<email> ...]" >&2
  exit 1
fi

ARN=$(aws sns create-topic --name "$TOPIC_NAME" --region "$REGION" --query TopicArn --output text)

for email in "$@"; do
  aws sns subscribe --topic-arn "$ARN" --protocol email \
    --notification-endpoint "$email" --region "$REGION" >/dev/null
  echo "Subscription requested for $email: open the confirmation email from AWS and click Confirm."
done

echo
echo "Topic ARN: $ARN"
echo "Next:"
echo "  1. Set the Airflow Variable alert_sns_topic_arn to the ARN above (Admin > Variables)."
echo "  2. Allow sns:Publish on it for the Airflow environment's IAM role (docs/ALERTING.md)."
