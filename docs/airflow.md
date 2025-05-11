# Apache Airflow: EC2 Self-Hosted vs Astronomer vs MWAA

Este documento compara las opciones de desplegar Apache Airflow en EC2, usar MWAA (Amazon Managed Workflows for Apache Airflow), o una plataforma como Astronomer, con foco en control, costos y facilidad de mantenimiento.

---

## 1. Comparativa general

| Característica            | EC2 Self-Hosted              | Amazon MWAA                     | Astronomer Cloud            |
| ------------------------- | ---------------------------- | ------------------------------- | --------------------------- |
| 📈 Escalabilidad          | Manual (Celery, Kubernetes)  | Automática limitada (por clase) | Muy alta, en Kubernetes     |
| 🔧 Mantenimiento          | Totalmente tuyo              | AWS lo gestiona                 | Astronomer lo gestiona      |
| 👛 Costo estimado mensual | \$10-25 USD (t3.micro + EBS) | \~\$100-500+ según clase        | \~\$300+ (según plan)       |
| 🚪 Seguridad              | Tú la configuras             | IAM y VPC integrados            | IAM, OAuth, RBAC, etc.      |
| 🌐 Acceso Web UI          | 8080 manual o con Nginx      | Por consola AWS                 | Desde Astronomer UI         |
| ⚙️ Integraciones AWS      | Requiere SDK / policies      | Nativas con Glue, Athena, etc   | Buenas, via providers       |
| 📊 Logging                | Local o CloudWatch (manual)  | CloudWatch automático           | Cloud con monitoreo central |
| 🤪 Curva de aprendizaje   | Alta                         | Baja-moderada                   | Baja                        |

---

## 2. Instalación en EC2 (Guía básica)

### Requisitos

* EC2 Amazon Linux 2 o Ubuntu
* Python 3.8+ / 3.10
* Seguridad:

  * Puerto 22 (SSH)
  * Puerto 8080 (Airflow UI opcional)

### Pasos de instalación

```bash
# Actualizar sistema
sudo yum update -y  # o sudo apt update en Ubuntu

# Instalar dependencias
sudo yum install git python3 -y
pip3 install --upgrade pip

# Variables
export AIRFLOW_HOME=~/airflow
export AIRFLOW_VERSION=2.7.3
export PYTHON_VERSION=3.10
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Instalar Airflow
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Inicializar
airflow db init

# Crear usuario admin
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Ejecutar webserver y scheduler
airflow webserver --port 8080 &
airflow scheduler &
```

---

## 3. Opcional: Infraestructura con Terraform para EC2 Airflow

Puedes usar Terraform para desplegar:

* EC2 t3.micro
* Security Group con puertos 22 y 8080
* EBS de 20 GB
* Instancia con `user_data` para instalar Airflow

> Esto reduce el trabajo manual y permite replicar ambientes fácilmente

---

## 4. Conclusión y recomendaciones

* Si tienes experiencia en ops y quieres **ahorrar costos**, EC2 es buena opción
* Si necesitas **baja fricción y soporte AWS nativo**, MWAA es fácil de usar
* Si trabajas en equipo con flujos complejos y buscas **escala y gobierno**, Astronomer es ideal

---

## 5. Diagrama de arquitectura (EC2 Self-Hosted)

```
[ Usuario Dev ]
     |
     v
[ EC2 Instance (t3.micro) ]
     | Airflow UI (8080)
     | Scheduler + Webserver
     | DAGs en ~/airflow/dags
     |
     v
[ Glue / Athena / S3 via boto3 ]
```

---

## 6. Archivos recomendados

* `~/airflow/requirements.txt` (si usas paquetes extra)
* `~/airflow/dags/` carpeta para tus flujos
* `.env` para variables locales si usas `dotenv`

---

# Terraform para EC2 con Apache Airflow preinstalado (Amazon Linux 2)

provider "aws" {
  region = "us-east-1"
  profile = "default"
}

resource "aws_key_pair" "airflow_key" {
  key_name   = "airflow-key"
  public_key = file("~/.ssh/id_rsa.pub")
}

resource "aws_security_group" "airflow_sg" {
  name        = "airflow-sg"
  description = "Allow SSH and Airflow Web UI"

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
  ami                    = "ami-0c2b8ca1dad447f8a" # Amazon Linux 2 AMI
  instance_type          = "t3.micro"
  key_name               = aws_key_pair.airflow_key.key_name
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]

  user_data = <<-EOF
              #!/bin/bash
              yum update -y
              yum install -y python3 git
              pip3 install --upgrade pip

              export AIRFLOW_HOME=/home/ec2-user/airflow
              export AIRFLOW_VERSION=2.7.3
              export PYTHON_VERSION=3.10
              export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

              pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINT_URL"
              airflow db init
              airflow users create --username admin --firstname Admin --lastname User \
                --role Admin --email admin@example.com --password admin

              nohup airflow webserver --port 8080 &
              nohup airflow scheduler &
              EOF

  tags = {
    Name = "airflow-ec2"
  }
}

output "public_ip" {
  value = aws_instance.airflow.public_ip
  description = "IP pública del servidor EC2 con Airflow"
}
