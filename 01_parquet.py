# !/usr/bin/python
# coding=utf-8
# =======================================================================================
#                         IEXE Tec - Cómputo de Datos Masivos
# Código fuente para el laboratorio 01: Formato de archivos Parquet.
# =======================================================================================

# Asegurate de correr 'pip install pyarrow pandas s3fs' para poder ejecutar el programa.
from pyarrow import csv
import pyarrow.parquet as pq
from s3fs import S3FileSystem

# Crear los objetos que controlan la lectura de archivos
parse_opts = csv.ParseOptions(delimiter='\t')  # "\t" denota el caracter tabulador
convert_opts = csv.ConvertOptions(strings_can_be_null=True)  # indica que los datos puden tener campos vacíos

# Se lee el archivo del sistema de archivos local
table = csv.read_csv('title.basics.tsv', parse_options=parse_opts, convert_options=convert_opts)

table = table.rename_columns(['tconst', 'titletype', 'primarytitle', 'originaltitle', 'isadult', 'startyear', 'endyear', 'runtimeminutes', 'genres'])

# Escribir algunas propiedades de la tabla
print(f'Nombre de columnas: {table.column_names}')
print(f'Bytes: {table.nbytes}')
print(f'Número de columnas: {table.num_columns}')
print(f'Número de registros: {table.num_rows}')
print(f'Shape: {table.shape}')
print(f'Esquema: {table.schema}')

# ---------------------------------------------------------------------------------------
# Primer archivo: Se escribe el archivo en formato Parquet sin compresión
pq.write_table(table, 'imdb_title_plain.parquet')

# ---------------------------------------------------------------------------------------
# Segundo archivo archivo: Mismos datos pero con compresión GZIP
# Consulta la documentación de PyArrow para conocer otros algoritmos de compresión de datos: 
# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
pq.write_table(table, 'imdb_title_gzip.parquet', compression='GZIP')

# ---------------------------------------------------------------------------------------
# Tercer archivo: Ahora se escriben particionando los datos por el tipo de película y 
# el año de lanzamiento. El argumento "partition_cols" indica una lista de columnas o
# campos en los datos para hacer la partición. 
pq.write_to_dataset(
    table, 
    compression='GZIP', 
    root_path='partitioned', 
    partition_cols=['titletype', 'startyear']
)

# ---------------------------------------------------------------------------------------
# Cuarto archivo: La misma estructura de partición de archivos se almacenan en S3
# Ahora el parámetro "filesystem" le indica a la biblioteca que use un bucket en S3
# como sistema de archivos para almacenar las particiones de archivos.
pq.write_to_dataset(
    table, 
    compression='GZIP', 
    root_path='iexetec-mcdia9999-imdb/title',  # reemplaza tu matrícula en esta línea 
    partition_cols=['titletype', 'startyear'], 
    filesystem=S3FileSystem()  # este objeto automáticamente toma tus credenciales de S3
)

# ---------------------------------------------------------------------------------------
# Inspeccionar los metadatos del archivo comprimido en GZIP
print('=' * 50)
print('Metadata')

metadata = pq.read_metadata('imdb_title_gzip.parquet')
print(metadata)

# Inspeccionar con más detalle las columnas del archivo
for row_group_idx in range(metadata.num_row_groups):
    row_group = metadata.row_group(row_group_idx)
    print('-' * 40)
    print(f'row_group {row_group_idx}')
    print(row_group)
    for num_column in range(row_group.num_columns):
        print('-' * 30)
        print(f'column {num_column}')
        print(row_group.column(num_column))
