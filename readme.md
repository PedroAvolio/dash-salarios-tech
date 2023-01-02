# Setup do ambiente
## Docker
* docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01

## Pastas
* .vscode
  * settings.json
  * launch.json

## Extensão do Vscode
* Remote Development
* Docker (Opcional)

## Permissão
* chmod +x startup.sh

## Informações Adicionais
* DataType
  * https://docs.aws.amazon.com/databrew/latest/dg/datatypes.html
* Bibliotecas já inclusas no Glue 3
  * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html
* Criar biblioteca Wheel
  * https://c-sharpcorner.com/article/create-python-wheel-file-and-deploy-production-pipelines-with-python-wheel-task/

```
# self.dynamicFrame = glue_context.create_dynamic_frame.from_options(
        #     connection_type="dynamodb",
        #     connection_options={
        #         "dynamodb.export": "ddb",
        #         "dynamodb.tableArn": "arn:aws:dynamodb:us-east-1:383129725783:table/staging.users",
        #         "dynamodb.unnestDDBJson": True,
        #         "dynamodb.s3.bucket": "network.cubo.datalake",
        #         "dynamodb.s3.prefix": "cubonetwork/raw/staging/testes",
        #     }
        # )
```

```# import boto3
# from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
# ts= TypeSerializer()
# td = TypeDeserializer()

# data= {"id": "5000"}
# serialized_data= ts.serialize(data)
# print(serialized_data)
# #{'M': {'id': {'S': '5000'}}}
# deserialized_data= td.deserialize(serialized_data)
# print(deserialized_data)
# #{'id': '5000'}
```