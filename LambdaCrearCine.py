import boto3
import json

def lambda_handler(event, context):
    try:
        print(event)

        # Extraer valores del cuerpo
        tenant_id = event['tenant_id']
        pais = event['pais']
        departamento = event['departamento']
        provincia = event['provincia']
        distrito = event['distrito']
        nombre = event['nombre']
        direccion = event['direccion']
        contacto = event['contacto']
        horario_apertura = event['horario_apertura']
        horario_cierre = event['horario_cierre']

        if not tenant_id and not pais and not departamento and not provincia and not distrito and not nombre and not direccion and not contacto and not horario_apertura and not horario_cierre:
            return {
                    'statusCode': 400,
                    'status': 'Bad Request - Faltan datos por completar'
                }

        # Concatenar los valores de pais, departamento y distrito para formar el campo ordenamiento
        ordenamiento = f"{pais}#{departamento}#{provincia}#{distrito}"

        # Proteger el Lambda con autenticación de token
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

        # Conexión a DynamoDB y creación del nuevo registro
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('tp_cines')  # Reemplaza con el nombre real de tu tabla

        # Insertar el nuevo registro en la tabla
        response = table.put_item(
            Item={
                'tenant_id': tenant_id,
                'ordenamiento': ordenamiento,
                'nombre': nombre,
                'direccion': direccion,
                'contacto': contacto,
                'horario_apertura': horario_apertura,
                'horario_cierre': horario_cierre
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
