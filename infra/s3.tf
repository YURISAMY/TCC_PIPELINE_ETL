
resource "aws_s3_bucket" "bronze" {
  bucket = "medallion-bronze"
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket" "silver" {
  bucket = "medallion-silver"
}

resource "aws_s3_bucket_versioning" "silver" {

  bucket = aws_s3_bucket.silver.id

  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket" "gold" {
  bucket = "medallion-gold"
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

resource "aws_s3_bucket_versioning" "gold" {

  bucket = aws_s3_bucket.gold.id

  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_policy" "gold" {

  bucket = aws_s3_bucket.gold.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "who_can_acess"
      Effect    = "Allow"
      Principal = "*"
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.gold.arn}/*"
    }]
  })
}
