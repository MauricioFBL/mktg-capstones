
resource "aws_key_pair" "airflow_key" {
  key_name   = "airflow-key"
  public_key = file("~/.ssh/airflow-key.pub")
}

resource "aws_security_group" "airflow_sg" {
  name        = "airflow-sg"
  description = "Permite acceso SSH y Airflow UI"

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

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "airflow" {
  ami           = "ami-0c02fb55956c7d316"
  instance_type = "t3.micro"
  key_name      = aws_key_pair.airflow_key.key_name
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_instance_profile.name  # <- Esta línea es clave

  root_block_device {
    volume_size = 20
  }

  tags = {
    Name = "airflow-ec2"
  }
}

output "ec2_public_ip" {
  value = aws_instance.airflow.public_ip
}