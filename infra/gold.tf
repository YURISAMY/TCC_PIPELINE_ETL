# 1. Cria a identidade (Role) que será usada pela Lambda da Gold

resource "aws_iam_role" "lambda_transform_to_gold" {
  name = "lambda-transform-to-gold-role"

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

resource "aws_iam_role_policy" "lambda_read_silver" {
  name = "lambda-read-silver-policy"
  role = aws_iam_role.lambda_transform_to_gold.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:GetObject"
      Resource = "${aws_s3_bucket.silver.arn}/*"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_put_in_gold" {
  name = "lambda-put-in-gold-policy"
  role = aws_iam_role.lambda_transform_to_gold.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = "${aws_s3_bucket.gold.arn}/*"
    }]
  })
}

#geração de logs...
resource "aws_iam_role_policy_attachment" "lambda_gold_logs" {
  role       = aws_iam_role.lambda_transform_to_gold.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "archive_file" "gold_load_zip" {
  type        = "zip"
  source_file = "${path.module}/../scripts/gold_etl.py"
  output_path = "${path.module}/../scripts/gold_etl_package.zip"
}

resource "aws_lambda_function" "gold_process" {
  function_name = "gold-process-lambda"
  role          = aws_iam_role.lambda_transform_to_gold.arn
  handler       = "gold_etl.lambda_handler"
  runtime       = "python3.12"

  
  # Descomentar a linha abaixo quando for mandar pra AWS
  # layers = [ "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:14" ]

  filename         = data.archive_file.gold_load_zip.output_path
  source_code_hash = data.archive_file.gold_load_zip.output_base64sha256 
  # calculo importante do ash, pois caso o script de processamento mude uma unica linha o terraform vai saber que precisa atualizar o codigo pra lambda.

  timeout     = 180  
  memory_size = 512 

  environment {
    variables = {
      BUCKET_SILVER_NAME = aws_s3_bucket.silver.id
      BUCKET_GOLD_NAME   = aws_s3_bucket.gold.id
    }
  }
}

# Autoriza o bucket Silver a invocar a Lambda da Gold
resource "aws_lambda_permission" "allow_bucket_silver_to_invoke_gold" {
  statement_id  = "AllowExecutionFromS3BucketSilver"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gold_process.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.silver.arn
}

# Gatilho do S3: quando um .parquet for criado em silver/, dispara a Lambda Gold tal qual acontece quando chega algo no bronze.
resource "aws_s3_bucket_notification" "silver_to_gold_trigger" {
  bucket = aws_s3_bucket.silver.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.gold_process.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "silver/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_bucket_silver_to_invoke_gold]
}