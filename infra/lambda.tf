
resource "aws_iam_role" "lambda_ingest_bronze" {
  //criei uma nova identidade
  name = "lambda-ingest-bronze-role"

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

//resumindo, eu criei uma intendidade, e o serviço lambda.amazonaws pode assumir essa identidade

resource "aws_iam_role_policy" "lambda_ingest_bronze_write" {
  name = "lambda-ingest-bronze-write-policy"
  role = aws_iam_role.lambda_ingest_bronze.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{

      // efeito : permite que quem assumir essa role faça oque está abaixo: criar objeto, no recurso bucket bronze. 

      Effect = "Allow"

      Action = "s3:PutObject"

      Resource = "${aws_s3_bucket.bronze.arn}/*"
    }]
  })
}

//Crio aqui a politica/regra da identidade eu criei acima, (role conecta essa regra a identidade acima).
//Ao assumir essa identidade, será possivel escrever objetos dentro do bucket bronze

data "archive_file" "ingest_bronze_zip" {
  type        = "zip"
  source_file = "${path.module}/../scripts/ingest_bronze.py"
  output_path = "${path.module}/../scripts/ingest_bronze_package.zip"
}

//pego o meu arquivo.py e o transformo num zip pois a lambda só aceita executar codigos de dentro de um pacote.

resource "aws_lambda_function" "ingest-bronze" {
  function_name = "ingest-lambda"
  role          = aws_iam_role.lambda_ingest_bronze.arn
  handler       = "ingest_bronze.ingest_bronze_def"
  runtime       = "python3.12"

  filename         = data.archive_file.ingest_bronze_zip.output_path
  source_code_hash = data.archive_file.ingest_bronze_zip.output_base64sha256

  timeout     = 60
  memory_size = 128

  environment {
    variables = {
      BUCKET_BRONZE_NAME = aws_s3_bucket.bronze.id
    }
  }
}

//de fato criei a função lambda, ela assume a identidade de lambda_ingest_bronze, consequentemente pode escrever dentro de bucket bronze. 
// usando python 3.12 executará o a função (def), ingest_bronze_def de dentro do arquivo ingest_bronze.py