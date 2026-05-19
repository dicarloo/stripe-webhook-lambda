# stripe-webhook-lambda

Procesador de webhooks de Stripe corriendo en AWS Lambda. Recibe eventos de pagos, suscripciones y reembolsos, los valida, los persiste en DynamoDB y publica notificaciones internas en SNS.

## El problema

Stripe manda eventos a un endpoint HTTP cada vez que pasa algo importante: un pago, un fallo, una suscripción nueva. El problema es que esos eventos pueden llegar duplicados, fuera de orden, o en rafagas. Necesitamos procesarlos de forma confiable sin cobrar dos veces ni perder datos.

## Arquitectura

```
Stripe
  │
  ▼
API Gateway  ──►  Lambda (este repo)
                      │
                      ├──► DynamoDB (tabla pagos)
                      ├──► DynamoDB (tabla pedidos)
                      ├──► DynamoDB (tabla eventos — idempotencia)
                      │
                      └──► SNS Topic
                                │
                                ├──► (tu servicio de emails)
                                └──► (tu servicio de inventario)
```

La Lambda valida la firma del webhook con el secret de Stripe, revisa si el evento ya fue procesado (idempotencia), ejecuta el handler correspondiente y publica un evento en SNS para que otros servicios reaccionen.

## Por qué Lambda

- El webhook de Stripe no llega constantemente, son rafagas. Lambda escala solo y no pagás por tiempo idle.
- No necesitás mantener un servidor corriendo solo para recibir eventos HTTP.
- El timeout de 30s es más que suficiente para escribir en Dynamo y publicar en SNS.
- SAM te da el deploy en un comando.

La alternativa que consideré fue ECS con un container siempre prendido, pero para un webhook que recibe cientos de eventos por día (no millones) es matar moscas a cañonazos.

## Eventos que maneja

| Evento Stripe | Qué hace |
|---|---|
| `payment_intent.succeeded` | Marca pedido como pagado, guarda pago |
| `payment_intent.payment_failed` | Marca pedido como fallido |
| `charge.refunded` | Registra reembolso parcial o total |
| `customer.subscription.created` | Registra suscripción nueva |
| `customer.subscription.deleted` | Marca suscripción como cancelada |
| `invoice.payment_failed` | Registra fallo de factura recurrente |

## Cómo correrlo

### Prerequisitos

```bash
# instalar SAM CLI
brew install aws-sam-cli

# credenciales de AWS
aws configure

# dependencias Python
pip install -r webhook_processor/requirements.txt
```

### Variables de entorno necesarias

```bash
STRIPE_SECRET_KEY=sk_live_...
STRIPE_WEBHOOK_SECRET=whsec_...
TABLA_PEDIDOS=pedidos
TABLA_PAGOS=pagos
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789:eventos-pagos
```

### Deploy

```bash
cd webhook_processor

sam build

sam deploy \
  --stack-name stripe-webhooks \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    StripeSecretKey=$STRIPE_SECRET_KEY \
    StripeWebhookSecret=$STRIPE_WEBHOOK_SECRET
```

La URL del endpoint aparece en los outputs del deploy. Esa URL la configurás en el [dashboard de Stripe](https://dashboard.stripe.com/webhooks) en la sección de webhooks.

### Probar local

```bash
sam local start-api

# en otra terminal
stripe listen --forward-to localhost:3000/webhook/stripe
```

### Probar un evento específico

```bash
stripe trigger payment_intent.succeeded
```

## Idempotencia

Cada evento de Stripe tiene un ID único (`evt_...`). Antes de procesarlo, la Lambda revisa si ese ID ya existe en la tabla `eventos_stripe`. Si ya está, devuelve 200 sin hacer nada. Los registros tienen TTL de 7 días.

## Estructura

```
webhook_processor/
├── handler.py       # lógica principal
├── template.yaml    # infraestructura SAM/CloudFormation
└── requirements.txt
```
