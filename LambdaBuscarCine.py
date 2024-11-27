import boto3
import json
import os

def lambda_handler(event, context):
    try:
        print(event)

        if event.get('motivo', 'buscar') == 'verificar':
            tenant_id = event['tenant_id']
            departamento = event['departamento']
            provincia = event['provincia']
            distrito = event['distrito']
        
        else:
            if isinstance(event['body'], str):
                body = json.loads(event['body'])
            else:
                body = event['body']
            
            # Extraer valores del cuerpo
        
            tenant_id = body['tenant_id']
            departamento = body.get('departamento', "") 
            provincia = body.get('provincia', "")       
            distrito = body.get('distrito', "") 

            # Validar el token de autorización
            token = event['headers'].get('Authorization', None)
            if not token:
                return {
                    'statusCode': 401,
                    'status': 'Unauthorized - Falta el token de autorización'
                }

        
            lambda_name = os.environ['LAMBDA_VALIDAR_TOKEN']
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

        tabla_cines = os.environ["TABLE_NAME_CINES"]

        # Conexión a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tabla_cines)

        # Construir la clave de ordenamiento
        ordenamiento_filter = f"{departamento}#{provincia}#{distrito}"
        print(f"Ordenamiento construido: {ordenamiento_filter}")

        # Configurar las expresiones para el Query
        key_condition_expression = "tenant_id = :tenant_id"
        expression_attribute_values = {":tenant_id": tenant_id}

        if ordenamiento_filter:
            key_condition_expression += " AND begins_with(cine_id, :cine_id)"
            expression_attribute_values[":cine_id"] = ordenamiento_filter

        # Ejecutar la consulta Query
        response = table.query(
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

        print(f"Respuesta de DynamoDB: {response}")

        if 'Items' in response and response['Items']:
            return {
                'statusCode': 200,
                'data': response['Items']
            }
        else:
            return {
                'statusCode': 404,
                'status': 'Not Found - No se encontraron cines con los criterios especificados'
            }

    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return {
            'statusCode': 500,
            'status': 'Internal Server Error - Ocurrió un error inesperado'
        }