# Terraform Project Guide – FCorp Data Platform

This document summarizes the Terraform setup and best practices for the `fcorp-data-prod` infrastructure used to manage AWS Glue Jobs, IAM roles, and S3 deployments.

---

## 🌍 Project Structure

```
marketing-analytics-pipeline/
├── infra/terraform/            # Terraform infrastructure code
├── jobs/                      # Glue scripts and job logic
├── .terraform/                # Terraform provider and backend state cache (IGNORED)
├── .terraform.lock.hcl        # Provider dependency lock file (SHOULD be committed)
└── ...
```

---

## ✅ Backend Configuration (Remote State)

Terraform is configured to use a **remote backend** with:

* **S3 bucket**: `fcorp-tfstate`
* **Key**: `terraform/fcorp-data-prod.tfstate`
* **Region**: `us-east-1`
* **DynamoDB table**: `fcorp-tf-lock`

**Purpose**:

* S3 stores the state file (`tfstate`) centrally
* DynamoDB ensures **locking** to avoid simultaneous changes

### DynamoDB Locking

* When you run `terraform plan` or `apply`, a lock is placed in `fcorp-tf-lock`
* If interrupted or failed, you may need to manually remove the lock
* To view: DynamoDB > `fcorp-tf-lock` > Explore items

---

## 🚀 Initialization

Before working with Terraform:

```bash
export AWS_PROFILE=mbautista
cd infra/terraform/
terraform init
terraform plan -var-file="terraform.tfvars"
```

To apply changes:

```bash
terraform apply -var-file="terraform.tfvars"
```

---

## 🔐 IAM and Policies

We define a Glue role with:

* `AWSGlueServiceRole`
* `AmazonAthenaFullAccess`
* `AWSGlueConsoleFullAccess`
* **Custom policy** for S3 prefix-based access (e.g., raw/, staging/, etc.)

**We removed** `AWSLambdaRole` because it does not exist. Use `AWSLambda_FullAccess` or a custom policy for Lambda if needed later.

---

## 🪣 S3 Structure (Single Bucket Strategy)

Bucket: `fcorp-data-prod`

Suggested prefix structure:

```
raw/
staging/
consumption/
scripts/glue/
scripts/lambda/
dags/
logs/
```

Use prefix-based IAM permissions to restrict access.

---

## 🧪 Glue Job Setup

* Version: `glue_version = "4.0"`
* Python 3.10 compatible
* `worker_type = "G.1X"`
* `number_of_workers = 2`
* Script uploaded to S3 via `aws_s3_object`

Default arguments (recommended):

```hcl
  default_arguments = {
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"  = "true"
    "--TempDir"                            = "s3://fcorp-data-prod/logs/temp/"
  }
```

---

## 🔎 Common Issues

### ❌ `filemd5` path error

Check that the relative path to your `.py` script is correct.

```hcl
source = "${path.module}/../../jobs/job/jobscript.py"
```

### ❌ `NoSuchEntity` on AWSLambdaRole

That policy does **not exist**. Use another or define a custom one.

### ❌ `Reference to undeclared module`

If you reference `module.*` but have no `module` defined, remove or comment out the output.

---

## 📘 Tips for Teams

* All infra should be applied **only after PR review**
* Use `terraform plan` as pre-step in CI/CD
* Locking ensures safe collaboration
* Store sensitive variables **outside of code** or encrypted

---

## 📁 Suggested Outputs

```hcl
output "glue_role_name" {
  value = aws_iam_role.glue_role.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}
```

---

## 🗂️ About `.terraform` and `.terraform.lock.hcl`

### `.terraform/`

* Contains downloaded providers and backend configuration cache
* **Should NOT be committed to git**
* Add to `.gitignore`:

```gitignore
.terraform/
```

### `.terraform.lock.hcl`

* Locks exact provider versions used by the project
* Ensures consistent builds across teams
* **SHOULD be committed to the repo**

---

## ✅ Summary

You have:

* Glue job automation
* IAM best practices
* Remote backend in S3 + DynamoDB
* Safe and scalable path to multi-user collaboration

Next steps:

* Modularize by component (Glue, Lambda, MWAA)
* Add GitHub Actions for `terraform plan`
* Expand to production workflows


# 🌐 Configuración de red para Amazon MWAA – Proyecto fcorp

Este documento describe paso a paso cómo configurar una red compatible con Amazon MWAA, incluyendo VPC, subredes, enrutamiento, y conectividad a internet.

---

## ✅ 1. Crear la VPC

- **Nombre**: `vpc-fcorp-mwaa`
- **CIDR IPv4**: `10.0.0.0/16`
- **Tenencia**: Predeterminado
- **IPv6**: Opcional, puedes omitirlo

---

## ✅ 2. Crear Subredes

| Nombre             | Zona de disponibilidad | CIDR IPv4      | Tipo     |
|--------------------|------------------------|----------------|----------|
| `fcorp-private-a`  | `us-east-1a`           | `10.0.50.0/24` | Privada  |
| `fcorp-private-b`  | `us-east-1b`           | `10.0.60.0/24` | Privada  |
| `fcorp-public-a`   | `us-east-1a`           | `10.0.70.0/24` | Pública  |

---

## ✅ 3. Crear y asociar componentes de red

### 🔸 Internet Gateway (IGW)

- **Nombre**: `igw-fcorp-mwaa`
- Asociado a `vpc-fcorp-mwaa`

### 🔸 Elastic IP

- Asignar una nueva dirección IP elástica (sin asociar)

### 🔸 NAT Gateway

- **Nombre**: `nat-fcorp-mwaa`
- **Subred**: `fcorp-public-a`
- **Elastic IP**: usar la creada anteriormente

---

## ✅ 4. Tablas de enrutamiento

### 🔸 Tabla pública (`rtb-public-fcorp`)

- Ruta: `0.0.0.0/0 → igw-fcorp-mwaa`
- Asociar subred: `fcorp-public-a`

### 🔸 Tabla privada (`rtb-private-fcorp`)

- Ruta: `0.0.0.0/0 → nat-fcorp-mwaa`
- Asociar subredes:
  - `fcorp-private-a`
  - `fcorp-private-b`

---

## ✅ 5. Verificación

| Subred             | Tipo     | Enrutamiento         |
|--------------------|----------|----------------------|
| `fcorp-private-a`  | Privada  | `rtb-private-fcorp`  |
| `fcorp-private-b`  | Privada  | `rtb-private-fcorp`  |
| `fcorp-public-a`   | Pública  | `rtb-public-fcorp`   |

---

## ✅ 6. ¿Y ahora qué?

Con esta infraestructura:

- Puedes usar MWAA sin errores de red
- Las subredes privadas tienen acceso a internet saliente vía NAT
- Las públicas sirven para exponer servicios o conectar NAT/IGW

Ahora puedes ir a la consola de **MWAA** y crear tu entorno seleccionando:

- VPC: `vpc-fcorp-mwaa`
- Subredes: `fcorp-private-a` y `fcorp-private-b`

# 💸 Estimación de costos mensuales – Amazon MWAA (`mw1.micro`)

Configuración optimizada para entornos de desarrollo o pruebas.

---

## ✅ Parámetros del entorno

| Componente                     | Valor configurado |
|--------------------------------|-------------------|
| Clase de entorno               | `mw1.micro`       |
| Scheduler (programador)        | 1                 |
| Workers (min y max)            | 1                 |
| Webservers (min y max)         | 1                 |
| CloudWatch Logs                | Habilitado        |
| Subredes privadas              | 2 en AZ distintas |
| Acceso al servidor web         | Privado           |

---

## 💰 Estimación mensual (24/7 encendido)

| Componente        | Precio aprox/hora | Estimación mensual (USD) |
|-------------------|-------------------|---------------------------|
| Scheduler (1x)    | $0.114            | ~$82.08                   |
| Worker (1x)       | $0.044            | ~$31.68                   |
| Webserver         | Incluido          | $0                        |
| CloudWatch logs   | ~0.50/GB          | ~$0.50 – $1.00            |
| Almacenamiento S3 | ~0.023/GB         | ~$0.10                    |

### 💵 **Total mensual estimado**:  
```bash
▶️ $115 – $120 USD / mes (24/7 activo)
