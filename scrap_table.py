import requests
import boto3
import uuid
import json
from datetime import datetime

def lambda_handler(event, context):
    # Año actual (puedes fijarlo a 2025 si quieres)
    year = datetime.now().year

    # Endpoint JSON del IGP (el que viste en Network)
    url = f"https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/{year}"

    # Llamar a la API JSON
    resp = requests.get(url, timeout=10)

    if resp.status_code != 200:
        return {
            "statusCode": resp.status_code,
            "body": json.dumps({
                "message": "Error al acceder a la API de sismos",
                "status_code": resp.status_code,
                "raw_sample": resp.text[:200]
            }, ensure_ascii=False)
        }

    # Parsear la respuesta como JSON
    try:
        data = resp.json()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "No se pudo parsear la respuesta como JSON",
                "error": str(e),
                "raw_sample": resp.text[:200]
            }, ensure_ascii=False)
        }

    # 🔴 AQUÍ ESTABA EL PROBLEMA
    # Si data es una lista → úsala directo.
    # Si es dict → intenta sacar "data".
    if isinstance(data, list):
        sismos = data
    elif isinstance(data, dict):
        sismos = data.get("data", [])
    else:
        sismos = []

    if not sismos:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "message": "No se encontraron sismos en la respuesta",
                "tipo_data": str(type(data))
            }, ensure_ascii=False)
        }

    # Opcional: si quieres ordenar, asumiendo que vienen estos campos
    sismos.sort(
        key=lambda x: (x.get("fecha_local", ""), x.get("hora_local", "")),
        reverse=True
    )

    # Nos quedamos solo con los 10 últimos sismos
    sismos_10 = sismos[:10]

    # Conexión a DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_dynamo = dynamodb.Table("TablaSismosIGP")

    # Limpiar la tabla antes de insertar
    scan = table_dynamo.scan()
    with table_dynamo.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar los 10 sismos
    for idx, sismo in enumerate(sismos_10, start=1):
        item = {
            "id": str(uuid.uuid4()),   # PK
            "numero": idx,            # orden 1..10

            # Campos principales (ajusta los nombres según la respuesta real)
            "magnitud":    sismo.get("magnitud", ""),
            "referencia":  sismo.get("referencia", ""),
            "fecha_local": sismo.get("fecha_local", ""),
            "hora_local":  sismo.get("hora_local", ""),
            "profundidad": sismo.get("profundidad", ""),
            "latitud":     sismo.get("latitud", ""),
            "longitud":    sismo.get("longitud", "")
        }
        table_dynamo.put_item(Item=item)

    # Retornar los 10 sismos
    return {
        "statusCode": 200,
        "body": json.dumps(sismos_10, ensure_ascii=False)
    }
