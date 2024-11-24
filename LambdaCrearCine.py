import boto3
import json
import os

def lambda_handler(event, context):
    try:
        print(event)

        if isinstance(event['body'], str):
            body = json.loads(event['body'])
        else:
            body = event['body']

        # Extraer valores del cuerpo
        tenant_id = body['tenant_id']
        departamento = body['departamento']
        provincia = body['provincia']
        distrito = body['distrito']
        nombre = body['nombre']
        direccion = body['direccion']
        contacto = body['contacto']
        imagen = body['imagen']

        tabla_cines = os.environ["TABLE_NAME_CINES"]

        if not tenant_id and not departamento and not provincia and not distrito and not nombre and not direccion and not contacto and not imagen:
            return {
                    'statusCode': 400,
                    'status': 'Bad Request - Faltan datos por completar'
                }

        # Concatenar los valores de pais, departamento y distrito para formar el campo ordenamiento
        ordenamiento = f"{departamento}#{provincia}#{distrito}"

        # Proteger el Lambda con autenticación de token
        token = event['headers'].get('Authorization', None)
        if not token:
            return {
                'statusCode': 401,
                'status': 'Unauthorized - Falta el token de autorización'
            }

        lambda_name = os.environ.get('LAMBDA_VALIDAR_TOKEN')

        lambda_client = boto3.client('lambda')
        payload_string = json.dumps({"token": token})
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

        # Conexión a DynamoDB y creación del nuevo registro
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tabla_cines)

        # Insertar el nuevo registro en la tabla
        response = table.put_item(
            Item={
                'tenant_id': tenant_id,
                'ordenamiento': ordenamiento,
                'nombre': nombre,
                'direccion': direccion,
                'contacto': contacto,
                'imagen': imagen
            }
        )

        # Respuesta de éxito
        return {
            'statusCode': 201,
            'status': 'Cine creado exitosamente',
            'response': response
        }

    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return {
            'statusCode': 500,
            'status': 'Internal Server Error - Ocurrió un error inesperado'
        }
