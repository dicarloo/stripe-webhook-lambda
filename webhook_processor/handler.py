import json
import os
import time
import stripe
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# import logging
# logging.basicConfig(level=logging.DEBUG)

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")

REGION = os.environ.get("AWS_REGION", "us-east-1")

dynamodb = boto3.resource("dynamodb")
tabla_pedidos = dynamodb.Table(os.environ.get("TABLA_PEDIDOS", "pedidos"))
tabla_pagos = dynamodb.Table(os.environ.get("TABLA_PAGOS", "pagos"))
tabla_eventos = dynamodb.Table(os.environ.get("TABLA_EVENTOS", "eventos_stripe"))

# sqs = boto3.client("sqs")
# COLA_URL = os.environ.get("SQS_COLA_URL")

sns = boto3.client("sns", region_name=REGION)
TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")


def lambda_handler(event, context):
    # print("evento completo:", json.dumps(event))

    cuerpo = event.get("body", "")
    if isinstance(cuerpo, bytes):
        cuerpo = cuerpo.decode("utf-8")

    headers = event.get("headers", {})

    firma = (
        headers.get("stripe-signature")
        or headers.get("Stripe-Signature")
        or headers.get("STRIPE-SIGNATURE")
    )

    if not firma:
        print("no vino firma de stripe")
        return _respuesta(400, {"error": "falta firma"})

    try:
        evento_stripe = stripe.Webhook.construct_event(
            cuerpo, firma, WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError as e:
        print("firma invalida:", str(e))
        return _respuesta(400, {"error": "firma invalida"})
    except Exception as e:
        print("error construyendo evento:", str(e))
        return _respuesta(500, {"error": "error interno"})

    tipo = evento_stripe["type"]
    evento_id = evento_stripe["id"]
    datos = evento_stripe["data"]["object"]

    # print("tipo de evento:", tipo)
    # print("datos:", json.dumps(datos, default=str))

    if _evento_ya_procesado(evento_id):
        print(f"evento duplicado ignorado: {evento_id}")
        return _respuesta(200, {"recibido": True, "duplicado": True})

    handlers = {
        "payment_intent.succeeded": _pago_exitoso,
        "payment_intent.payment_failed": _pago_fallido,
        "charge.refunded": _reembolso,
        "customer.subscription.created": _suscripcion_creada,
        "customer.subscription.deleted": _suscripcion_cancelada,
        "invoice.payment_failed": _factura_fallida,
    }

    # if tipo not in handlers:
    #     print(f"evento ignorado: {tipo}")
    #     return _respuesta(200, {"recibido": True})

    fn = handlers.get(tipo)
    if fn:
        try:
            fn(datos, evento_stripe)
            _marcar_evento_procesado(evento_id, tipo)
        except Exception as e:
            print(f"error procesando {tipo}:", str(e))
            return _respuesta(500, {"error": "fallo al procesar"})
    else:
        print(f"evento no manejado: {tipo}")

    return _respuesta(200, {"recibido": True})


def _pago_exitoso(datos, evento):
    payment_intent_id = datos["id"]
    monto = datos["amount"]
    moneda = datos["currency"]
    cliente_id = datos.get("customer")
    metadata = datos.get("metadata", {})
    pedido_id = metadata.get("pedido_id")
    descripcion = datos.get("description", "")

    # print("payment_intent_id:", payment_intent_id)
    # print("pedido_id de metadata:", pedido_id)
    # print("descripcion:", descripcion)

    ahora = datetime.utcnow().isoformat()

    tabla_pagos.put_item(Item={
        "pago_id": payment_intent_id,
        "pedido_id": pedido_id or "sin_pedido",
        "cliente_id": cliente_id or "desconocido",
        "monto": monto,
        "moneda": moneda,
        "descripcion": descripcion,
        "estado": "exitoso",
        "creado_en": ahora,
        "evento_id": evento["id"],
    })

    if pedido_id:
        # antes actualizaba directo con update_item pero tiraba error si no existia el pedido
        # tabla_pedidos.update_item(
        #     Key={"pedido_id": pedido_id},
        #     UpdateExpression="SET estado = :e",
        #     ExpressionAttributeValues={":e": "pagado"}
        # )
        try:
            tabla_pedidos.update_item(
                Key={"pedido_id": pedido_id},
                UpdateExpression="SET estado = :e, pagado_en = :f, pago_id = :p",
                ConditionExpression="attribute_exists(pedido_id)",
                ExpressionAttributeValues={
                    ":e": "pagado",
                    ":f": ahora,
                    ":p": payment_intent_id,
                },
            )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            print(f"pedido {pedido_id} no existe en dynamo, solo guardamos el pago")

    _publicar_evento("pago_exitoso", {
        "pedido_id": pedido_id,
        "pago_id": payment_intent_id,
        "monto": monto,
        "moneda": moneda,
    })


def _pago_fallido(datos, evento):
    payment_intent_id = datos["id"]
    metadata = datos.get("metadata", {})
    pedido_id = metadata.get("pedido_id")
    ultimo_error = datos.get("last_payment_error", {})
    razon = ultimo_error.get("message", "sin detalle")
    codigo = ultimo_error.get("code", "")

    # print("razon fallo:", razon)
    # print("codigo error:", codigo)

    tabla_pagos.put_item(Item={
        "pago_id": payment_intent_id,
        "pedido_id": pedido_id or "sin_pedido",
        "estado": "fallido",
        "razon": razon,
        "codigo_error": codigo,
        "creado_en": datetime.utcnow().isoformat(),
        "evento_id": evento["id"],
    })

    if pedido_id:
        tabla_pedidos.update_item(
            Key={"pedido_id": pedido_id},
            UpdateExpression="SET estado = :e, error_pago = :r",
            ExpressionAttributeValues={
                ":e": "pago_fallido",
                ":r": razon,
            },
        )

    _publicar_evento("pago_fallido", {
        "pedido_id": pedido_id,
        "pago_id": payment_intent_id,
        "razon": razon,
    })


def _reembolso(datos, evento):
    charge_id = datos["id"]
    monto_reembolsado = datos["amount_refunded"]
    reembolsado_total = datos["refunded"]
    payment_intent_id = datos.get("payment_intent")
    metadata = datos.get("metadata", {})
    pedido_id = metadata.get("pedido_id")

    # intenté sacar el pedido_id del payment_intent pero es un paso extra
    # pi = stripe.PaymentIntent.retrieve(payment_intent_id)
    # pedido_id = pi.metadata.get("pedido_id")

    estado_nuevo = "reembolsado" if reembolsado_total else "reembolso_parcial"

    tabla_pagos.put_item(Item={
        "pago_id": charge_id,
        "pedido_id": pedido_id or "sin_pedido",
        "estado": estado_nuevo,
        "monto_reembolsado": monto_reembolsado,
        "creado_en": datetime.utcnow().isoformat(),
        "evento_id": evento["id"],
    })

    if pedido_id:
        tabla_pedidos.update_item(
            Key={"pedido_id": pedido_id},
            UpdateExpression="SET estado = :e",
            ExpressionAttributeValues={":e": estado_nuevo},
        )

    _publicar_evento("reembolso", {
        "pedido_id": pedido_id,
        "charge_id": charge_id,
        "monto_reembolsado": monto_reembolsado,
        "total": reembolsado_total,
    })


def _suscripcion_creada(datos, evento):
    sub_id = datos["id"]
    cliente_id = datos["customer"]
    plan_id = datos["items"]["data"][0]["price"]["id"] if datos.get("items") else None
    estado = datos["status"]

    # print("suscripcion nueva:", sub_id, "cliente:", cliente_id)

    tabla_pagos.put_item(Item={
        "pago_id": sub_id,
        "cliente_id": cliente_id,
        "plan_id": plan_id or "desconocido",
        "tipo": "suscripcion",
        "estado": estado,
        "creado_en": datetime.utcnow().isoformat(),
        "evento_id": evento["id"],
    })

    _publicar_evento("suscripcion_creada", {
        "sub_id": sub_id,
        "cliente_id": cliente_id,
        "plan_id": plan_id,
        "estado": estado,
    })


def _suscripcion_cancelada(datos, evento):
    sub_id = datos["id"]
    cliente_id = datos["customer"]
    cancelado_en = datos.get("canceled_at")
    motivo = datos.get("cancellation_details", {}).get("reason", "")

    # no siempre viene canceled_at si la cancelas al final del periodo
    # print("cancelado_en:", cancelado_en)
    # print("motivo cancelacion:", motivo)

    tabla_pagos.update_item(
        Key={"pago_id": sub_id},
        UpdateExpression="SET estado = :e, cancelado_en = :c, motivo = :m",
        ExpressionAttributeValues={
            ":e": "cancelada",
            ":c": str(cancelado_en) if cancelado_en else "desconocido",
            ":m": motivo or "sin_motivo",
        },
    )

    _publicar_evento("suscripcion_cancelada", {
        "sub_id": sub_id,
        "cliente_id": cliente_id,
    })


def _factura_fallida(datos, evento):
    invoice_id = datos["id"]
    cliente_id = datos["customer"]
    sub_id = datos.get("subscription")
    monto = datos.get("amount_due")
    intento = datos.get("attempt_count", 0)

    print(f"factura fallida {invoice_id}, intento #{intento}")

    if intento >= 3:
        _publicar_evento("suscripcion_en_riesgo", {
            "invoice_id": invoice_id,
            "cliente_id": cliente_id,
            "sub_id": sub_id,
            "intentos": intento,
        })

    tabla_pagos.put_item(Item={
        "pago_id": invoice_id,
        "cliente_id": cliente_id,
        "sub_id": sub_id or "sin_sub",
        "monto": monto,
        "tipo": "factura_fallida",
        "intento": intento,
        "estado": "fallido",
        "creado_en": datetime.utcnow().isoformat(),
        "evento_id": evento["id"],
    })

    _publicar_evento("factura_fallida", {
        "invoice_id": invoice_id,
        "cliente_id": cliente_id,
        "sub_id": sub_id,
        "intento": intento,
    })


def _publicar_evento(tipo_interno, payload, reintentos=3):
    if not TOPIC_ARN:
        print("sin TOPIC_ARN configurado, saltando SNS")
        return

    # antes no tenia reintentos y a veces sns tiraba throttling en prod
    for intento in range(reintentos):
        try:
            sns.publish(
                TopicArn=TOPIC_ARN,
                Message=json.dumps(payload),
                Subject=tipo_interno,
                MessageAttributes={
                    "tipo_evento": {
                        "DataType": "String",
                        "StringValue": tipo_interno,
                    }
                },
            )
            return
        except ClientError as e:
            codigo = e.response["Error"]["Code"]
            if codigo == "Throttling" and intento < reintentos - 1:
                time.sleep(2 ** intento)
                continue
            print(f"error publicando en SNS ({tipo_interno}):", str(e))
            break
        except Exception as e:
            print(f"error publicando en SNS ({tipo_interno}):", str(e))
            break

    # alternativa con SQS que probe antes
    # sqs.send_message(
    #     QueueUrl=COLA_URL,
    #     MessageBody=json.dumps({"tipo": tipo_interno, "payload": payload}),
    # )


def _evento_ya_procesado(evento_id):
    try:
        resp = tabla_eventos.get_item(Key={"evento_id": evento_id})
        return "Item" in resp
    except Exception as e:
        print("error chequeando idempotencia:", str(e))
        return False


def _marcar_evento_procesado(evento_id, tipo):
    try:
        tabla_eventos.put_item(Item={
            "evento_id": evento_id,
            "tipo": tipo,
            "procesado_en": datetime.utcnow().isoformat(),
            # ttl de 7 dias para no llenar la tabla para siempre
            "ttl": int(datetime.utcnow().timestamp()) + 604800,
        })
    except Exception as e:
        print("error marcando evento procesado:", str(e))


def _respuesta(status, cuerpo):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(cuerpo),
    }
