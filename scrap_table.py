import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": json.dumps({"message": "Error al acceder a la página web"})
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, "html.parser")

    # Encontrar la tabla en el HTML
    table = soup.find("table")
    if not table:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "No se encontró la tabla en la página web"})
        }

    # Extraer los encabezados de la tabla
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    else:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]

    # Extraer las filas de la tabla
    rows = []
    tbody = table.find("tbody") or table  # por si no hay <tbody>
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue  # salta filas vacías

        row_dict = {}
        for i, cell in enumerate(cells):
            if i < len(headers):
                col_name = headers[i]
            else:
                col_name = f"col_{i}"
            row_dict[col_name] = cell.get_text(strip=True)

        rows.append(row_dict)

    # Nos quedamos solo con los 10 últimos sismos
    rows = rows[:10]

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    # 👇 AQUÍ CAMBIÉ EL NOMBRE DE LA TABLA
    table_dynamo = dynamodb.Table("TablaSismosIGP")

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table_dynamo.scan()
    with table_dynamo.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(
                Key={
                    "id": item["id"]
                }
            )

    # Insertar los nuevos datos
    for index, row in enumerate(rows, start=1):
        row["#"] = index
        row["id"] = str(uuid.uuid4())
        table_dynamo.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        "statusCode": 200,
        "body": json.dumps(rows, ensure_ascii=False)
    }
