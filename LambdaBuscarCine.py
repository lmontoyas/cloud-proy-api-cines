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

        # Extraer parámetros de búsqueda del evento
        query_params = event.get('queryStringParameters', {})
        print("queryStringParameters:", query_params)  # Imprimir los parámetros de la URL

        tenant_id = query_params.get('tenant_id', None)
        departamento = query_params.get('departamento', None)
        provincia = query_params.get('provincia', None)
        distrito = query_params.get('distrito', None)

        print("Parámetros extraídos:", tenant_id, departamento, provincia, distrito)  # Imprimir los valores extraídos

        tabla_cines = os.environ["TABLE_NAME_CINES"]
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

        # Conexión a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tabla_cines)

        # Construir la clave de ordenamiento
        ordenamiento_filter = f"{departamento}#{provincia}#{distrito}"

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

        # Retornar los resultados
        return {
            'statusCode': 200,
            'status': 'Búsqueda exitosa',
            'data': response.get('Items', [])
        }

    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return {
            'statusCode': 500,
            'status': 'Internal Server Error - Ocurrió un error inesperado'
        }