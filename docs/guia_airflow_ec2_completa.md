# Guía completa para configurar Apache Airflow en EC2 (Terraform + Bash + GitHub Actions)

Esta guía explica paso a paso cómo desplegar Apache Airflow en EC2 de forma segura y automatizada, utilizando:
- Terraform (infraestructura)
- AWS SSM Parameter Store (credenciales)
- Bash script (instalación de Airflow)
- GitHub Actions (carga de DAGs)

---

## ✅ Paso 1: Generar par de llaves SSH

En tu máquina local:
```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/airflow-key
```
Esto generará:
- `~/.ssh/airflow-key`       → Clave privada (NO se sube a GitHub)
- `~/.ssh/airflow-key.pub`   → Clave pública

---

## ✅ Paso 2: Crear infraestructura con Terraform

### Archivo `ec2.tf`
```hcl
provider "aws" {
  region = "us-east-1"
}

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

  root_block_device {
    volume_size = 20
  }

  tags = {
    Name = "airflow-ec2"
  }
}

resource "aws_ssm_parameter" "airflow_admin" {
  name  = "/airflow/admin"
  type  = "SecureString"
  value = jsonencode({
    username  = "admin"
    firstname = "Admin"
    lastname  = "User"
    email     = "admin@example.com"
    password  = "admin123"
  })
}

output "ec2_public_ip" {
  value = aws_instance.airflow.public_ip
}
```

### Comandos para aplicar:
```bash
terraform init
terraform apply
```

---

## ✅ Paso 3: Conectarse a la instancia EC2

Desde tu máquina:
```bash
chmod 400 ~/.ssh/airflow-key
ssh -i ~/.ssh/airflow-key ec2-user@<IP_PUBLICA>
```

---

## ✅ Paso 4: Crear y ejecutar `setup_airflow.sh` en EC2

Este script:
- Instala Python, Airflow, jq, awscli
- Recupera credenciales desde Parameter Store
- Inicializa Airflow y lanza Webserver y Scheduler

### Comando:
```bash
vim setup_airflow.sh   # o nano, o subir desde tu máquina con scp
chmod +x setup_airflow.sh
./setup_airflow.sh
```

Al final, Airflow quedará expuesto en:
```text
http://<ip-publica-ec2>:8080
```

---

## ✅ Paso 5: Configurar GitHub Actions para subir DAGs

### 1. Añadir secretos en GitHub:
- `EC2_HOST`: IP pública
- `EC2_SSH_KEY`: Contenido del archivo `airflow-key.pem` (clave privada)

### 2. Crear workflow `.github/workflows/deploy_dags_ec2.yml`:
```yaml
name: Deploy DAGs to EC2

on:
  push:
    branches:
      - main

jobs:
  sync-dags:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Sync DAGs to EC2
        uses: appleboy/scp-action@v0.1.4
        with:
          host: ${{ secrets.EC2_HOST }}
          username: ec2-user
          key: ${{ secrets.EC2_SSH_KEY }}
          source: "dags/*.py"
          target: "~/airflow/dags/"
          strip_components: 1
```

---

## ✅ Paso 6: Validar que todo funcione

1. Confirma acceso a `http://<ip>:8080`
2. Accede con las credenciales de `/airflow/admin`
3. Haz un push a `main` y valida que los DAGs aparezcan

---

Con esta guía puedes levantar un entorno completo de Airflow controlado por código, económico y seguro, ideal para desarrollo o prototipos.
