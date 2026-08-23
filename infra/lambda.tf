
resource "aws_iam_role" "lambda_ingest_bronze" {
  name = "lambda-ingest-bronze-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_ingest_bronze_write" {
  name = "lambda-ingest-bronze-write-policy"
  role = aws_iam_role.lambda_ingest_bronze.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = "${aws_s3_bucket.bronze.arn}/*"
    }]
  })
}

data "archive_file" "ingest_bronze_zip" {
  type        = "zip"
  source_file = "${path.module}/../scripts/ingest_bronze.py"
  output_path = "${path.module}/../scripts/ingest_bronze_package.zip"
}

resource "aws_lambda_function" "ingest-bronze" {
  function_name = "ingest-lambda"
  role          = aws_iam_role.lambda_ingest_bronze.arn
  handler       = "ingest_bronze.ingest_bronze_def"
  runtime       = "python3.12"

  filename         = data.archive_file.ingest_bronze_zip.output_path
  source_code_hash = data.archive_file.ingest_bronze_zip.output_base64sha256

  timeout     = 60
  memory_size = 128
}