
resource "aws_s3_bucket" "bronze" {
  bucket = "tcc-ministack-medallion-bronze"
}


resource "aws_s3_bucket" "silver" {
  bucket = "tcc-ministack-medallion-silver"
}

resource "aws_s3_bucket_versioning" "silver" {

  bucket = aws_s3_bucket.silver.id

  versioning_configuration { status = "Enabled" }
}


resource "aws_s3_bucket" "gold" {
  bucket = "tcc-ministack-medallion-gold"
}

resource "aws_s3_bucket_versioning" "gold" {

  bucket = aws_s3_bucket.gold.id

  versioning_configuration { status = "Enabled" }
}