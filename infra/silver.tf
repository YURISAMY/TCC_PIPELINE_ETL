// camada silver - Transformação


// resumo: transcrevendo o que acontece em todas essas linhas. o primeiro bloco cria uma nova identidade,
//que pode ser assumida pelo serviço lambda, e os blocos seguintes definem que essa lambda vai poder tanto ler o conteudo do bronze,
//quanto escrever no bucket silver, e por fim fazer logs para ficar claro os processos internos que ocorrem.

resource "aws_iam_role" "lambda_transform_to_silver" {
  //criei uma nova identidade
  name = "lambda-transform-to-silver-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      // "é permitido assumir-essa-role, e quem pode fazer isso é quem esta no principal"
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}


resource "aws_iam_role_policy" "lambda_read_bronze" {

  name = "lambda-read-bronze-policy"
  role = aws_iam_role.lambda_transform_to_silver.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{

      // efeito : permite que quem assumir essa role faça oque está abaixo: criar objeto, no recurso bucket bronze. 

      Effect = "Allow"

      Action = "s3:GetObject"

      Resource = "${aws_s3_bucket.bronze.arn}/*"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_put_in_silver" {

  name = "lambda-put-in-silver-policy"
  role = aws_iam_role.lambda_transform_to_silver.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{

      // efeito : permite que quem assumir essa role faça oque está abaixo: criar objeto, no recurso bucket bronze. 

      Effect = "Allow"

      Action = "s3:PutObject"

      Resource = "${aws_s3_bucket.silver.arn}/*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_silver_logs" {
  role       = aws_iam_role.lambda_transform_to_silver.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Declara a função Lambda de transformação
resource "aws_lambda_function" "silver_process" {
  function_name = "silver-process-lambda"
  role          = aws_iam_role.lambda_transform_to_silver.arn
  handler       = "silver_load.lambda_handler"
  runtime       = "python3.12"

#PARA AWS

  #layers = [
  #  "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:14"
  #]

#LAYER LOCAL

# No bloco resource "aws_lambda_function" "silver_process":
# layers = [
#   aws_lambda_layer_version.pandas_local.arn
# ]

  filename         = data.archive_file.silver_load_zip.output_path
  source_code_hash = data.archive_file.silver_load_zip.output_base64sha256

  timeout     = 180
  memory_size = 512

  environment {
    variables = {
      BUCKET_BRONZE_NAME = aws_s3_bucket.bronze.id
      BUCKET_SILVER_NAME = aws_s3_bucket.silver.id
      CHAVE_ZIP          = "bronze/raw/postos.zip"
    }
  }
}

resource "aws_lambda_permission" "allow_bucket_bronze_to_invoke_silver" {
  statement_id = "AllowExecutionFromS3BucketBronze"

  // pode ser qualquer nome

  action = "lambda:InvokeFunction"

  // oque pode fazer

  function_name = aws_lambda_function.silver_process.function_name

  //nome da função que é a que ta ali em cima

  principal = "s3.amazonaws.com"

  // quem terá essa permissão

  source_arn = aws_s3_bucket.bronze.arn

  //quem pode fazer isso? 
}


resource "aws_s3_bucket_notification" "bronze_to_silver_trigger" {
  bucket = aws_s3_bucket.bronze.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.silver_process.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "bronze/raw/"
    filter_suffix       = ".zip"
  }

  depends_on = [aws_lambda_permission.allow_bucket_bronze_to_invoke_silver]
}

resource "aws_lambda_layer_version" "pandas_local" {
  filename            = "${path.module}/local_layer/pandas_layer.zip"
  layer_name          = "pandas-pyarrow-local-layer"
  compatible_runtimes = ["python3.12"]
  source_code_hash    = filebase64sha256("${path.module}/local_layer/pandas_layer.zip")
}

data "archive_file" "silver_load_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../scripts"
  output_path = "${path.module}/../scripts/silver_load_package.zip"
  excludes    = ["*.zip", "__pycache__", "old script.ipynb", "teste_de_precisão.py", "inspecionar.py", "gerar_csv.py"]
}