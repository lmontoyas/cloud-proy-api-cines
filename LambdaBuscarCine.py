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

        lambda_client = boto3.client('lambda')
        payload_string = json.dumps({"token": token})
        invoke_response = lambda_client.invoke(
            FunctionName="ValidarTokenAcceso",
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

        # Extraer parámetros de búsqueda del evento
        query_params = event.get('queryStringParameters', {})
        tenant_id = query_params.get('tenant_id', None)
        departamento = query_params.get('departamento', None)
        provincia = query_params.get('provincia', None)
        distrito = query_params.get('distrito', None)

        tabla_cines = os.environ["TABLE_NAME_CINES"]

        # Conexión a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tabla_cines)

        # Buscar por tenant_id (método query)
        if tenant_id:
            key_condition_expression = "tenant_id = :tenant_id"
            expression_attribute_values = {":tenant_id": tenant_id}

            if departamento or provincia or distrito:
                # Agregar filtro de ordenamiento si hay otros parámetros
                ordenamiento_filter = f"{departamento or ''}#{provincia or ''}#{distrito or ''}".strip("#")
                if ordenamiento_filter:
                    key_condition_expression += " AND begins_with(ordenamiento, :ordenamiento)"
                    expression_attribute_values[":ordenamiento"] = ordenamiento_filter

            response = table.query(
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeValues=expression_attribute_values
            )

        else:
            # Buscar sin tenant_id (método scan)
            filter_expression = []
            expression_attribute_values = {}

            if departamento:
                filter_expression.append("begins_with(ordenamiento, :departamento)")
                expression_attribute_values[":departamento"] = departamento
            if provincia:
                filter_expression.append("contains(ordenamiento, :provincia)")
                expression_attribute_values[":provincia"] = f"#{provincia}#"
            if distrito:
                filter_expression.append("ends_with(ordenamiento, :distrito)")
                expression_attribute_values[":distrito"] = f"#{distrito}"

            if filter_expression:
                filter_expression = " AND ".join(filter_expression)
                response = table.scan(
                    FilterExpression=filter_expression,
                    ExpressionAttributeValues=expression_attribute_values
                )
            else:
                response = table.scan()

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
