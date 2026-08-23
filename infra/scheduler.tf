//quero agora um agendador pra ficar chamando a função lambda_ingest_bronze

resource "aws_iam_role" "lambda_scheduler" {
  name = "lambda_scheduler_bronze"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
    }]
  })
}

//crio aqui uma nova identidade que é o agendador lambda, ele permite que o serviço agendador da amazonaws assuma essa função/role

resource "aws_iam_role_policy" "scheduler_bronze_policy" {
  name = "lambda_scheduler_to_bronze_policy"
  role = aws_iam_role.lambda_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.ingest-bronze.arn
    }]
  })
}

//as regras dessa nova identidade, são que uma vez assumindo ela, é possivel invocar a função ingest-bronze, que é uma lambda criada em lambda.tf. 

resource "aws_scheduler_schedule" "funceme_ingest_scheduler" {
  name = "funceme_ingest_schedule"

  flexible_time_window {
    mode = "OFF"
  }
  //mudei para 1 minuto para ver funcionando
  schedule_expression = "rate(1 minutes)"

  target {
    arn      = aws_lambda_function.ingest-bronze.arn
    role_arn = aws_iam_role.lambda_scheduler.arn
  }
}

// o agendador de ingestão da funceme a cada 24 horas, assume a role lambda scheduler e chama o ingest-bronze