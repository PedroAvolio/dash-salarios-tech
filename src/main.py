import boto3
import logging
import sys
from boto3 import client
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import Map, DropFields, ApplyMapping


class Main:
    def __init__(self):
        params = []

        logging.info(sys.argv)
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
        if '--ENVIRONMENT' in sys.argv:
            params.append('ENVIRONMENT')
        args = getResolvedOptions(sys.argv, params)
        logging.info(args)

        self.s3client:boto3.client = boto3.client('s3')
        self.sc = SparkContext()
        self.context = GlueContext(self.sc)
        self.job = Job(self.context)

        if 'JOB_NAME' in args:
            jobname = args['JOB_NAME']
        else:
            jobname = "test"

        if 'ENVIRONMENT' in args:
            self.ENVIRONMENT = args['ENVIRONMENT']
        else:
            self.ENVIRONMENT = "prod"

        self.job.init(jobname, args)

    def run(self):

        # Limpeza das pastas
        self.clearFolder()
        
        # # Selo Cubo
        # self.read_data(self.returnReadDataPath("Selo Cubo Startups", "Selo Cubo 2023", "📝 Respostas Selo Cubo"))
        # self.processData()
        # self.write_data(self.returnWriteDataPath("Selo Cubo Startups", "Selo Cubo 2023", "📝 Respostas Selo Cubo"))

        # # Companies
        # self.read_data(self.returnReadDataPath("Selo Cubo Startups", "Corporates Ativas", "Grid view"))
        # self.processCompaniesAndPartnersData()
        # self.write_data(self.returnWriteDataPath("Selo Cubo Startups", "Corporates Ativas", "Grid view"))

        # # Parceiros
        # self.read_data(self.returnReadDataPath("Selo Cubo Startups", "Parceiros Ativos", "Grid view"))
        # self.processCompaniesAndPartnersData()
        # self.write_data(self.returnWriteDataPath("Selo Cubo Startups", "Parceiros Ativos", "Grid view"))

        # # Startups
        # self.read_data(self.returnReadDataPath("Selo Cubo Startups", "Startups Ativas", "Portfolio"))
        # self.processCompaniesAndPartnersData()
        # self.write_data(self.returnWriteDataPath("Selo Cubo Startups", "Startups Ativas", "Portfolio"))

        # # Create Latest
        # self.createLatest()

        self.job.commit()

    def processData(self):
        mapping = []
        for item in self.dataframe.unnest().toDF().dtypes:
            if item[0].split('.')[0] == "fields":
                if len(item[0].split(".")) == 2:
                    mapping.append((item[0], item[0].split('.')[1]))
            else:
                mapping.append((item[0], item[0].split('.')[0]))

        self.dataframe = self.dataframe.apply_mapping(mapping).resolveChoice(specs=[
            ("Faturamento 2022", "cast:decimal"),
            ("Valor a captar / Faturamento previsto", "cast:decimal"),
            ("Previsão do faturamento 2023", "cast:decimal"),
            ("Captable", "cast:decimal"),
            ("Faturamento 2021", "cast:decimal"),
            ("Turnover", "cast:decimal"),
            ("Δ Fat 21-22", "cast:decimal"),
            ("Δ Clientes 21-22", "cast:decimal"),
            ("Δ Fat 22-23", "cast:decimal"),
            ("Δ Fat 21-23", "cast:decimal"),
            ("Δ Fat 21-22/22-23", "cast:decimal"),
            ("Δ Clientes 22-23", "cast:decimal"),
            ("Δ Clientes 21-23", "cast:decimal"),
            ("Δ Clientes 21-22/22-23", "cast:decimal"),
            ("Δ Time 22-23", "cast:decimal"),
            ("Δ Time 21-22/22-23", "cast:decimal"),
            ("PERFORMANCE", "cast:decimal"),
            ("CRESCIMENTO", "cast:decimal"),
            ("INVESTIMENTO", "cast:decimal"),
            ("PESSOAS", "cast:decimal"),
            ("Δ Time 21-23", "cast:decimal"),
            ("Δ Time 21-22", "cast:decimal"),
            ("Δ Time 21-22/22-23", "cast:decimal")
        ])

    def processCompaniesAndPartnersData(self):
        mapping = []
        for item in self.dataframe.unnest().toDF().dtypes:
            if item[0].split('.')[0] == "fields":
                if len(item[0].split(".")) == 2:
                    mapping.append((item[0], item[0].split('.')[1]))
            else:
                mapping.append((item[0], item[0].split('.')[0]))

        self.dataframe = self.dataframe.apply_mapping(mapping)

    def clearFolder(self):
        # Apago a data de processamento
        paginator = self.s3client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.returnBucketName(), Prefix=self.partialNameProc() + self.findLastDate() + "/")

        files_to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    files_to_delete.append({"Key": obj["Key"]})
                    if len(files_to_delete) >= 800:
                        self.s3client.delete_objects(
                            Bucket=self.returnBucketName(), Delete={"Objects": files_to_delete}
                        )
                        files_to_delete = []

        if len(files_to_delete) > 0:
            self.s3client.delete_objects(
                Bucket=self.returnBucketName(), Delete={"Objects": files_to_delete}
            )
            

        # Apaga o recente
        paginator = self.s3client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.returnBucketName(), Prefix=self.partialNameProc() + "latest" + "/")

        files_to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page['Contents']:
                    if len(files_to_delete) <= 800:
                        files_to_delete.append({"Key": obj["Key"]})
                        if len(files_to_delete) >= 800:
                            self.s3client.delete_objects(
                                Bucket=self.returnBucketName(), Delete={"Objects": files_to_delete}
                            )
                            files_to_delete = []

        if len(files_to_delete) > 0:
            self.s3client.delete_objects(
                Bucket=self.returnBucketName(), Delete={"Objects": files_to_delete}
            )

    def createLatest(self):
        # Origem
        response = self.s3client.list_objects_v2(Bucket=self.returnBucketName(), Prefix=self.partialNameProc() + self.findLastDate() + "/")
        files_in_folder = []

        if "Contents" in response:
            files_in_folder = response["Contents"]

            for f in files_in_folder:
                response = self.s3client.copy({'Bucket': self.returnBucketName(), "Key": f["Key"]},
                                                Bucket=self.returnBucketName(), 
                                                Key=f["Key"].replace(self.findLastDate(), "latest"))

        return True

    def partialNameRaw(self):
        return "airtable/raw/" + self.ENVIRONMENT + "/"

    def partialNameProc(self):
        return "airtable/proc/" + self.ENVIRONMENT + "/"

    def returnBucketName(self):
        return "network.cubo.datalake"

    def returnReadDataPath(self, database, table, view):
        lastDate = self.findLastDate()
        return "s3://" + self.returnBucketName() + "/" + self.partialNameRaw() + lastDate + "/data/" + database + "/" + table + "/" + view + "/"

    def returnWriteDataPath(self, database, table, view):
        lastDate = self.findLastDate()
        return "s3://" + self.returnBucketName() + "/" + self.partialNameProc() + lastDate + "/data/" + database + "/" + table + "/" + view + "/"

    def findLastDate(self):
        toReturn = self.s3client.list_objects_v2(Bucket=self.returnBucketName(), Prefix=self.partialNameRaw(), Delimiter="/")
        folders = []
        for item in toReturn['CommonPrefixes']:
            folders.append(item["Prefix"].split("/")[len(item["Prefix"].split("/")) - 2])
        folders.reverse()
        return folders[0]

    def write_data(self, path):
        self.context.write_dynamic_frame.from_options(
            frame=self.dataframe,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": path,
                "partitionKeys": [],
            },
            format_options={"compression": "gzip"},
            transformation_ctx="S3Destination",
        )

    def read_data(self, path):
        self.dataframe = self.context.create_dynamic_frame.from_options(
            connection_type='s3',
            connection_options={
                'paths': [path],
                'recurse': True
            },
            format='json'
        )

def readMap(rec):
    print(rec)
    return rec

def readItem(value):
    print(value)
    return value

if __name__ == '__main__':
    Main().run()
