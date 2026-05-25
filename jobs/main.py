import boto3

from datetime import datetime
from functools import wraps
from typing import List, Tuple
import time

from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    current_timestamp,
    date_format,
    lit,
    trim,
    when
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType
)
# =========================================================
# Decorator para medir tempo de execução das funções
# =========================================================

def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()

        result = func(*args, **kwargs)

        end_time = time.time()
        execution_time = end_time - start_time

        print(
            f"[INFO] Função '{func.__name__}' executada em "
            f"{execution_time:.2f} segundos"
        )

        return result

    return wrapper
# =========================================================
# Inicialização da sessão Spark/Glue
# =========================================================

@measure_time
def init_spark_session() -> Tuple[GlueContext, SparkSession]:
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark_session = glue_context.spark_session

    spark_session.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

    spark_session.conf.set(
        "spark.sql.caseSensitive",
        "true"
    )

    print("[INFO] Sessão Spark inicializada com sucesso")

    return glue_context, spark_session
# =========================================================
# Função para gerar path particionado com base na data atual
# =========================================================

# =========================================================
# Busca a partição mais recente disponível no S3
# =========================================================

@measure_time
def get_latest_partition_path(
    bucket_name: str,
    layer: str,
    dataset: str
) -> str:

    s3_client = boto3.client("s3")

    prefix = f"{layer}/{dataset}/"

    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    if "Contents" not in response:
        raise ValueError(
            f"[ERROR] Nenhum arquivo encontrado em: s3://{bucket_name}/{prefix}"
        )

    latest_file = max(
        response["Contents"],
        key=lambda obj: obj["LastModified"]
    )

    latest_key = latest_file["Key"]

    partition_path = latest_key.rsplit("/", 1)[0] + "/"

    full_path = f"s3://{bucket_name}/{partition_path}"

    print(f"[INFO] Partição mais recente encontrada: {full_path}")

    return full_path
@measure_time
def extract(
    spark: SparkSession,
    input_path: str,
    schema: StructType
) -> DataFrame:

    try:

        print(f"[INFO] Iniciando extração dos dados: {input_path}")

        df = (
            spark.read
            .schema(schema)
            .option("multiline", "true")
            .json(input_path)
        )

        print(f"[INFO] Extração concluída com sucesso")

        return df

    except Exception as e:

        print(f"[ERROR] Erro durante extract: {str(e)}")

        raise
# =========================================================
# Transform — seleção e padronização inicial das colunas
# =========================================================

@measure_time
def select_required_columns(df: DataFrame) -> DataFrame:

    return df.select(
        col("id").alias("id_poke"),

        col("name.english").alias("nm_poke"),

        col("type")[0].alias("ds_tp_1"),
        col("type")[1].alias("ds_tp_2"),

        col("base.HP").alias("nr_pdv"),
        col("base.Attack").alias("nr_atq"),
        col("base.Defense").alias("nr_def"),
        col("base.`Sp. Attack`").alias("nr_atq_esp"),
        col("base.`Sp. Defense`").alias("nr_def_esp"),
        col("base.Speed").alias("nr_vel"),

        col("species").alias("ds_epc"),

        col("profile.height").alias("nr_alt"),
        col("profile.weight").alias("nr_pes")
    )
# =========================================================
# Adiciona colunas faltantes conforme schema esperado
# =========================================================

@measure_time
def add_missing_columns(
    df: DataFrame,
    schema: StructType
) -> DataFrame:

    for field in schema.fields:

        if field.name not in df.columns:

            df = df.withColumn(
                field.name,
                lit(None).cast(field.dataType)
            )

    return df
@measure_time
def remove_duplicate_rows(df: DataFrame) -> DataFrame:

    try:

        print("[INFO] Verificando registros duplicados")

        total_rows = df.count()

        df_deduplicated = df.dropDuplicates()

        deduplicated_rows = df_deduplicated.count()

        duplicated_rows = total_rows - deduplicated_rows

        print(f"[INFO] Registros duplicados encontrados: {duplicated_rows}")

        return df_deduplicated

    except Exception as e:

        print(f"[ERROR] Erro durante deduplicação: {str(e)}")

        raise
@measure_time
def apply_trim(df: DataFrame) -> DataFrame:
    """
    Aplica trim em todas as colunas string do DataFrame.

    Remove espaços em branco:
    - no início;
    - no final.
    """

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for column_name in string_columns:
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )

    return df
# =========================================================
# Adiciona coluna de referência no formato ANOMESDIA
# =========================================================

@measure_time
def add_reference_date(
    df: DataFrame,
    column_name: str = "dt_referencia"
) -> DataFrame:

    reference_date = int(
        datetime.today().strftime("%Y%m%d")
    )

    df = df.withColumn(
        column_name,
        lit(reference_date)
    )

    return df
# =========================================================
# Adiciona timestamp de processamento
# =========================================================

@measure_time
def add_timestamp_column(
    df: DataFrame,
    column_name: str = "dt_processamento"
) -> DataFrame:

    df = df.withColumn(
        column_name,
        current_timestamp()
    )

    return df
@measure_time
def transform(df: DataFrame) -> DataFrame:

    try:
        print("[INFO] Iniciando transformação dos dados")

        df_transformed = (
            df
            .transform(select_required_columns)
            .transform(apply_trim)
            .transform(remove_duplicate_rows)
            .transform(add_reference_date)
            .transform(add_timestamp_column)
            .transform(add_partition_columns)
        )

        print("[INFO] Transformação concluída com sucesso")

        return df_transformed

    except Exception as e:
        print(f"[ERROR] Erro durante transform: {str(e)}")
        raise
@measure_time
def validate_data(
    df: DataFrame,
    required_columns: List[str]
) -> Tuple[DataFrame, DataFrame]:

    try:

        print("[INFO] Iniciando validação dos dados")

        total_rows = df.count()

        print(f"[INFO] Total de registros recebidos: {total_rows}")

        print("[INFO] Quantidade de nulos por coluna:")

        nulls_by_column = df.select([
            count(
                when(col(column_name).isNull(), column_name)
            ).alias(column_name)
            for column_name in df.columns
        ]).collect()[0].asDict()

        for column_name, null_count in nulls_by_column.items():

            if null_count > 0:

                print(
                    f"[INFO] Coluna {column_name}: "
                    f"{null_count} nulos"
                )

        valid_condition = None
        invalid_condition = None

        for column_name in required_columns:

            current_valid_condition = col(column_name).isNotNull()

            current_invalid_condition = col(column_name).isNull()

            if valid_condition is None:

                valid_condition = current_valid_condition
                invalid_condition = current_invalid_condition

            else:

                valid_condition = (
                    valid_condition
                    & current_valid_condition
                )

                invalid_condition = (
                    invalid_condition
                    | current_invalid_condition
                )

        df_valid = df.filter(valid_condition)

        df_invalid = df.filter(invalid_condition)

        print(f"[INFO] Registros válidos: {df_valid.count()}")

        print(f"[INFO] Registros inválidos: {df_invalid.count()}")

        print("[INFO] Validação concluída com sucesso")

        return df_valid, df_invalid

    except Exception as e:

        print(f"[ERROR] Erro durante validate_data: {str(e)}")

        raise
@measure_time
def add_partition_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de partição para escrita no S3.

    Estrutura:
    year=YYYY/month=MM/day=DD
    """

    try:
        print("[INFO] Adicionando colunas de partição")

        df_partitioned = (
            df
            .withColumn("year", date_format(current_date(), "yyyy"))
            .withColumn("month", date_format(current_date(), "MM"))
            .withColumn("day", date_format(current_date(), "dd"))
        )

        print("[INFO] Colunas de partição adicionadas com sucesso")

        return df_partitioned

    except Exception as e:
        print(f"[ERROR] Erro ao adicionar colunas de partição: {str(e)}")
        raise
@measure_time
def load(
    df: DataFrame,
    output_path: str,
    partition_columns: List[str]
) -> None:
    """
    Escreve os dados válidos na camada processed do S3.

    Formato:
    - Parquet

    Compressão:
    - GZIP

    Particionamento:
    - year/month/day
    """

    try:
        print("[INFO] Iniciando etapa de carga dos dados")
        print(f"[INFO] Caminho de saída: {output_path}")
        print(f"[INFO] Colunas de partição: {partition_columns}")

        total_rows = df.count()

        print(f"[INFO] Total de registros para escrita: {total_rows}")

        (
            df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "gzip")
            .partitionBy(*partition_columns)
            .save(output_path)
        )

        print("[INFO] Carga concluída com sucesso")

    except Exception as e:
        print(f"[ERROR] Erro durante load: {str(e)}")
        raise
# =========================================================
# Configurações principais do pipeline
# =========================================================

BUCKET_NAME = "pokemon-glue-project-akemi"
RAW_LAYER = "raw"
PROCESSED_LAYER = "processed"
DATASET_NAME = "pokemons"

processed_path = f"s3://{BUCKET_NAME}/{PROCESSED_LAYER}/{DATASET_NAME}/"

partition_columns = [
    "year",
    "month",
    "day"
]

required_columns = [
    "id_poke",
    "nm_poke",
    "ds_tp_1"
]
pokemon_schema = StructType([

    StructField("id", IntegerType(), True),

    StructField("name", StructType([
        StructField("english", StringType(), True),
    ]), True),

    StructField("type", ArrayType(StringType()), True),

    StructField("base", StructType([
        StructField("HP", IntegerType(), True),
        StructField("Attack", IntegerType(), True),
        StructField("Defense", IntegerType(), True),
        StructField("Sp. Attack", IntegerType(), True),
        StructField("Sp. Defense", IntegerType(), True),
        StructField("Speed", IntegerType(), True),
    ]), True),

    StructField("species", StringType(), True),

    StructField("profile", StructType([
        StructField("height", StringType(), True),
        StructField("weight", StringType(), True),
    ]), True),
])
# =========================================================
# 1. Abrir Spark / Glue
# =========================================================

glueContext, spark = init_spark_session()

job = Job(glueContext)
# =========================================================
# 2. Paths
# =========================================================

raw_path = get_latest_partition_path(
    bucket_name=BUCKET_NAME,
    layer=RAW_LAYER,
    dataset=DATASET_NAME
)

print(raw_path)
print(processed_path)
# =========================================================
# 3. Extract
# =========================================================

df_raw = extract(
    spark=spark,
    input_path=raw_path,
    schema=pokemon_schema
)
# =========================================================
# 4. Transform
# =========================================================

df_transformed = transform(df_raw)

df_valid, df_invalid = validate_data(
    df=df_transformed,
    required_columns=required_columns
)
# =========================================================
# 5. Load
# =========================================================
load(
    df=df_valid,
    output_path=processed_path,
    partition_columns=partition_columns
)
job.commit()