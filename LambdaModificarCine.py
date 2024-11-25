import boto3
import json
import os

def lambda_handler(event, context):
    try:
        print(event)

        # Validar el token de autorización
        token = event['headers'].get('Authorization', None)
        if not token:
            return {
                'statusCode': 401,
                'status': 'Unauthorized - Falta el token de autorización'
            }
        
        if isinstance(event['body'], str):
            body = json.loads(event['body'])
        else:
            body = event['body']

        # Extraer datos del cuerpo del evento
        tenant_id = body['tenant_id']
        departamento = body['departamento']
        provincia = body['provincia']
        distrito = body['distrito']
        nombre = body['nombre']
        direccion = body['direccion']
        contacto = body['contacto']
        imagen = body['imagen']

        tabla_cines = os.environ["TABLE_NAME_CINES"]
        lambda_name = os.environ.get('LAMBDA_VALIDAR_TOKEN')

        # Validar que los campos requeridos están presentes
        if not tenant_id or not departamento or not provincia or not distrito:
            return {
                'statusCode': 400,
                'status': 'Bad Request - Faltan campos requeridos'
            }

        # Concatenar los valores de departamento, provincia y distrito para formar el campo cine_id
        cine_id = f"{departamento}#{provincia}#{distrito}"

        lambda_client = boto3.client('lambda')
        payload_string = json.dumps(
            {
                "tenant_id": tenant_id,
                "token": token
             })
        invoke_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType='RequestResponse',
            Payload=payload_string
        )
        response = json.loads(invoke_response['Payload'].read())
        print(response)
        if response['statusCode'] == 403:
            return {
                'statusCode': 403,
                'status': 'Forbidden - Acceso NO Autorizado'
            }

        # Conexión a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tabla_cines) 

        # Verificar si el registro existe
        existing_item = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'cine_id': cine_id
            }
        )
        if 'Item' not in existing_item:
            return {
                'statusCode': 404,
                'status': 'Not Found - El cine especificado no existe'
            }

        # Realizar la consulta Query
        query_response = table.query(
            KeyConditionExpression="tenant_id = :tenant_id AND begins_with(cine_id, :cine_id)",
            ExpressionAttributeValues={
                ":tenant_id": tenant_id,
                ":cine_id": cine_id
            }
        )

        # Verificar si se encontró al menos un ítem
        items = query_response.get('Items', [])
        if not items:
            return {
                'statusCode': 404,
                'status': 'Not Found - No se encontraron cines con los criterios especificados'
            }

        # Tomar el primer ítem encontrado (en este caso asumimos que es único)
        item_to_update = items[0]

        # Construir la expresión de actualización
        update_expression = []
        expression_attribute_values = {}
        if nombre:
            update_expression.append("nombre = :nombre")
            expression_attribute_values[":nombre"] = nombre
        if direccion:
            update_expression.append("direccion = :direccion")
            expression_attribute_values[":direccion"] = direccion
        if contacto:
            update_expression.append("contacto = :contacto")
            expression_attribute_values[":contacto"] = contacto
        if imagen:
            update_expression.append("imagen = :imagen")
            expression_attribute_values[":imagen"] = imagen

        if not update_expression:
            return {
                'statusCode': 400,
                'status': 'Bad Request - No se proporcionaron campos para actualizar'
            }

        # Ejecutar la actualización
        response = table.update_item(
            Key={
                'tenant_id': tenant_id,
                'cine_id': item_to_update['cine_id']
            },
            UpdateExpression="SET " + ", ".join(update_expression),
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="ALL_NEW"
        )

        # Retornar la respuesta con los datos actualizados
        return {
            'statusCode': 200,
            'status': 'Cine actualizado exitosamente',
            'updated_item': response.get('Attributes', {})
        }

    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return {
            'statusCode': 500,
            'status': 'Internal Server Error - Ocurrió un error inesperado'
        }