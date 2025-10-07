import os

import dotenv
import pymysql

dotenv.load_dotenv()

# Variables
HOST = os.environ['MYSQL_HOST']
USER = os.environ['MYSQL_USER']
PASSWORD = os.environ['MYSQL_PASSWORD']
DATABASE = os.environ['MYSQL_DATABASE']

connection = pymysql.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
)

with connection:
    with connection.cursor() as cursor:
        # SQL
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS hunters ('
            'id INT NOT NULL AUTO_INCREMENT, '
            'name VARCHAR(50) NOT NULL, '
            'age INT NOT NULL, '
            'division VARCHAR(50) NOT NULL, '
            'PRIMARY KEY (id) '
            ') '
        )
        connection.commit()
