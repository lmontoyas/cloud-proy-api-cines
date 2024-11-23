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

        # Extraer datos del cuerpo del evento
        body = json.loads(event.get('body', '{}'))
        tenant_id = body.get('tenant_id')
        departamento = body.get('departamento')
        provincia = body.get('provincia')
        distrito = body.get('distrito')
        nombre = body.get('nombre')
        direccion = body.get('direccion')
        contacto = body.get('contacto')
        horario_apertura = body.get('horario_apertura')
        horario_cierre = body.get('horario_cierre')
        imagen = body.get('imagen')

        tabla_cines = os.environ["TABLE_NAME_CINES"]

        # Validar que los campos requeridos están presentes
        if not tenant_id or not departamento or not provincia or not distrito:
            return {
                'statusCode': 400,
                'status': 'Bad Request - Faltan campos requeridos'
            }

        # Generar clave de ordenamiento
        ordenamiento = f"{departamento}#{provincia}#{distrito}"

        # Conexión a DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tabla_cines) 

        # Verificar si el registro existe
        existing_item = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'ordenamiento': ordenamiento
            }
        )
        if 'Item' not in existing_item:
            return {
                'statusCode': 404,
                'status': 'Not Found - El cine especificado no existe'
            }

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
        if horario_apertura:
            update_expression.append("horario_apertura = :horario_apertura")
            expression_attribute_values[":horario_apertura"] = horario_apertura
        if horario_cierre:
            update_expression.append("horario_cierre = :horario_cierre")
            expression_attribute_values[":horario_cierre"] = horario_cierre
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
                'ordenamiento': ordenamiento
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
