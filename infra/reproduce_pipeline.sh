#!/usr/bin/env bash
set -eo pipefail

################################################################################
# === CONFIGURE ONCE ===
# Replace the placeholder values below with your own, then run ./infra/reproduce_pipeline.sh
################################################################################

# AWS region & account
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID="123456789012"

# HealthLake
DATASTORE_ID="ENTER_YOUR_HEALTHLAKE_DATASTORE_ID"
HEALTHLAKE_ROLE="HealthLakeServiceRole"

# S3: bucket and prefix for export
EXPORT_BUCKET="my-fhir-export-bucket"
EXPORT_PREFIX="export"

# KMS: (will be created by this script)
# GLUE
GLUE_DATABASE="my_fhir_db"
GLUE_CRAWLER="my_fhir_crawler"
GLUE_ROLE="GlueCrawlerRole"

################################################################################
# === DEPLOY STEPS (do not change below) ===
################################################################################

# 1) Create S3 bucket (no-op if already exists)
aws s3 mb s3://$EXPORT_BUCKET --region $AWS_REGION

# 2) Create a customer‑managed KMS key
KMS_KEY_ARN=$(aws kms create-key \
  --description "FHIR export key" \
  --key-usage ENCRYPT_DECRYPT \
  --query 'KeyMetadata.Arn' --output text)

# 3) Attach managed policies to HealthLake role
aws iam attach-role-policy \
  --role-name $HEALTHLAKE_ROLE \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
  --role-name $HEALTHLAKE_ROLE \
  --policy-arn arn:aws:iam::aws:policy/AWSKeyManagementServicePowerUser

# 4) Create inline policy for S3+KMS export permissions
cat > infra/s3-export-permissions.json << EOF
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Sid":"ListBucket",
      "Effect":"Allow",
      "Action":["s3:ListBucket","s3:GetBucketPublicAccessBlock","s3:GetEncryptionConfiguration"],
      "Resource":["arn:aws:s3:::$EXPORT_BUCKET"]
    },
    {
      "Sid":"PutObjects",
      "Effect":"Allow",
      "Action":["s3:PutObject"],
      "Resource":["arn:aws:s3:::$EXPORT_BUCKET/$EXPORT_PREFIX/*"]
    },
    {
      "Sid":"UseKMS",
      "Effect":"Allow",
      "Action":["kms:Decrypt","kms:GenerateDataKey*","kms:DescribeKey","kms:ReEncrypt*"],
      "Resource":["$KMS_KEY_ARN"]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name $HEALTHLAKE_ROLE \
  --policy-name ExportS3KmsPermissions \
  --policy-document file://infra/s3-export-permissions.json

# 5) Start the FHIR export job
aws healthlake start-fhir-export-job \
  --region $AWS_REGION \
  --datastore-id $DATASTORE_ID \
  --output-data-config "{
    \"S3Configuration\": {
      \"S3Uri\":\"s3://$EXPORT_BUCKET/$EXPORT_PREFIX/\",
      \"KmsKeyId\":\"$KMS_KEY_ARN\"
    }
  }" \
  --data-access-role-arn arn:aws:iam::$AWS_ACCOUNT_ID:role/$HEALTHLAKE_ROLE

# 6) Create Glue catalog database
aws glue create-database \
  --database-input "{\"Name\":\"$GLUE_DATABASE\"}" \
  --region $AWS_REGION

# 7) Define Glue crawler
aws glue create-crawler \
  --name $GLUE_CRAWLER \
  --role $GLUE_ROLE \
  --database-name $GLUE_DATABASE \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$EXPORT_BUCKET/$EXPORT_PREFIX/\"}]}"

# 8) Run the crawler
aws glue start-crawler --name $GLUE_CRAWLER
