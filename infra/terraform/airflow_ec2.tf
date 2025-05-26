resource "aws_key_pair" "airflow_key_w" {
  key_name   = "airflow-key-no-pass"
  public_key = var.public_key
}

# NETWORK & VPC
resource "aws_vpc" "airflow_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "airflow-vpc" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.airflow_vpc.id
}


resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.airflow_vpc.id
}

resource "aws_route" "default_route" {
  route_table_id         = aws_route_table.public_rt.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

resource "aws_route_table_association" "public_rt_assoc" {
  subnet_id      = aws_subnet.public_subnet_1.id
  route_table_id = aws_route_table.public_rt.id
}

# SECURITY GROUPS
resource "aws_security_group" "airflow_sg" {
  name        = "airflow-sg"
  description = "Permite acceso SSH y Airflow UI"
  vpc_id      = aws_vpc.airflow_vpc.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = 5432
    to_port   = 5432
    protocol  = "tcp"
    self      = true
  }

  ingress {
    from_port = 8793
    to_port   = 8793
    protocol  = "tcp"
    self      = true # Para el scheduler si se comunica por RPC
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
# EC2 FOR AIRFLOW WEBSERVER
resource "aws_instance" "airflow" {
  ami                    = "ami-0c02fb55956c7d316"
  instance_type          = "t3.medium"
  key_name               = aws_key_pair.airflow_key_w.key_name
  subnet_id              = aws_subnet.public_subnet_1.id
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_instance_profile.name # <- Esta línea es clave

  associate_public_ip_address = true

  user_data = <<-EOF
              #!/bin/bash
              yum update -y
              amazon-linux-extras install docker -y
              service docker start
              usermod -a -G docker ec2-user
              curl -SL https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
              chmod +x /usr/local/bin/docker-compose
              mkdir -p /home/ec2-user/airflow/{dags,logs,plugins}
              chmod -R 777 /home/ec2-user/airflow
              EOF

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name = "airflow-webserver"
  }

  root_block_device {
    volume_size = 20
  }
}

output "ec2_public_ip" {
  value = aws_instance.airflow.public_ip
}

resource "aws_instance" "airflow_scheduler" {
  ami                    = "ami-0c02fb55956c7d316"
  instance_type          = "t3.micro"
  key_name               = aws_key_pair.airflow_key_w.key_name
  subnet_id              = aws_subnet.public_subnet_1.id
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_instance_profile.name

  associate_public_ip_address = true


  user_data = <<-EOF
              #!/bin/bash
              yum update -y
              amazon-linux-extras install docker -y
              service docker start
              usermod -a -G docker ec2-user
              curl -SL https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
              chmod +x /usr/local/bin/docker-compose
              mkdir -p /home/ec2-user/airflow/{dags,logs,plugins}
              chmod -R 777 /home/ec2-user/airflow
              EOF

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name = "airflow-scheduler"
  }
  root_block_device {
    volume_size = 10
  }


}

output "scheduler_public_ip" {
  value = aws_instance.airflow_scheduler.public_ip
}


resource "aws_db_instance" "airflow_rds" {
  identifier              = "airflow-postgres"
  engine                  = "postgres"
  engine_version          = "15.7"
  instance_class          = "db.t3.micro"
  allocated_storage       = 20
  username                = "airflow"
  password                = var.db_password
  db_name                 = "airflow"
  skip_final_snapshot     = true
  publicly_accessible     = true
  storage_encrypted       = false
  vpc_security_group_ids  = [aws_security_group.airflow_sg.id]
  db_subnet_group_name    = aws_db_subnet_group.airflow_subnets.name
  backup_retention_period = 0

  tags = {
    Name = "airflow-db"
  }
}

resource "aws_subnet" "public_subnet_1" {
  vpc_id                  = aws_vpc.airflow_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true
  tags = {
    Name = "public-subnet-1"
  }
}

resource "aws_subnet" "public_subnet_2" {
  vpc_id                  = aws_vpc.airflow_vpc.id
  cidr_block              = "10.0.3.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true
  tags = {
    Name = "public-subnet-2"
  }
}

resource "aws_db_subnet_group" "airflow_subnets" {
  name = "airflow-subnet-group"
  subnet_ids = [
    aws_subnet.public_subnet_1.id,
    aws_subnet.public_subnet_2.id
  ]
}

output "rds_endpoint" {
  value = aws_db_instance.airflow_rds.endpoint
}
