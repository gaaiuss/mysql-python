import os

import dotenv
import pymysql
import pymysql.cursors

dotenv.load_dotenv()

# Connection Variables
HOST = os.environ['MYSQL_HOST']
USER = os.environ['MYSQL_USER']
PASSWORD = os.environ['MYSQL_PASSWORD']
DATABASE = os.environ['MYSQL_DATABASE']

connection = pymysql.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    # Changes the default cursor to a Dict return
    cursorclass=pymysql.cursors.DictCursor,
    # SSCursor / SSDict Cursor for great number of data (Unbuffered)
    # cursorclass=pymysql.cursors.SSDictCursor,
)

# SQL Variables
TABLE = 'hunters'

with connection:  # pymysql has a context manager, so I can use "with"

    # -------------------------- CREATE TABLE ---------------------------------
    with connection.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE IF NOT EXISTS {TABLE} ('
            'id INT NOT NULL AUTO_INCREMENT, '
            'name VARCHAR(50) NOT NULL, '
            'age INT NOT NULL, '
            'unit VARCHAR(50) NOT NULL, '
            'PRIMARY KEY (id) '
            ') '
        )
        # WARNING - Clear table
        cursor.execute(f'TRUNCATE TABLE {TABLE}')

    # -------------------------- INSERT DATA ----------------------------------

    # Insert data using placeholders
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE} '
            '(name, age, unit) '
            'VALUES '
            '(%s, %s, %s) '  # Placeholder
        )
        data = ('Yoruichi', 25, 'Avis')  # Filling placeholder
        result = cursor.execute(sql, data)
        # print(sql)
        # print(data)
        # print('Number of affected rows:', result)
        connection.commit()  # Have to commit every change made on the database

    # Insert data using dictionaries
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE} '
            '(name, age, unit) '
            'VALUES '
            # The placeholder must match the dict key
            '(%(name)s, %(age)s, %(unit)s) '
        )
        data2 = {
            'name': 'Kurosaki Ichigo',
            'age': 15,
            'unit': 'Avis',
        }
        result = cursor.execute(sql, data2)
        # print(sql)
        # print(data2)
        # print('Number of affected rows:', result)
        connection.commit()  # Have to commit every change made on the database

    # Inserting with execute many (Inserting many values in the same query)
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE} '
            '(name, age, unit) '
            'VALUES '
            # The placeholder must match the dict key
            '(%(name)s, %(age)s, %(unit)s) '
        )
        data3 = (
            {'name': 'Emerald Kotar', 'age': 33, 'unit': 'Avis', },
            {'name': 'Nashi', 'age': 22, 'unit': 'Avis', },
            {'name': 'Boss Ygdran', 'age': 26, 'unit': 'Avis', },
        )
        result = cursor.executemany(sql, data3)  # type: ignore
        # print(sql)
        # print(data3)
        # print('Number of affected rows:', result)
        connection.commit()  # Have to commit every change made on the database

    # Inserting with execute many (Inserting many values in the same query)
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE} '
            '(name, age, unit) '
            'VALUES '
            # The placeholder must match the dict key
            '(%s, %s, %s) '
        )
        data4 = (
            ('Olivia', 20, 'Avis', ),
            ('Alma', 22, 'Avis', ),
        )
        result = cursor.executemany(sql, data4)  # type: ignore
        # print(sql)
        # print(data4)
        # print('Number of affected rows:', result)
        connection.commit()  # Have to commit every change made on the database

    # ----------------------------- READ DATA ---------------------------------

    with connection.cursor() as cursor:
        sql = (
            f'SELECT * FROM {TABLE} '
        )
        cursor.execute(sql)

        data5 = cursor.fetchall()

        # print(type(data5)) # Tuple

        # for row in data5:
        #     print(row)

    # SELECT Using WHERE
    with connection.cursor() as cursor:
        # id_received = input('Input an ID: ')
        id_received = 1
        collumn = 'id'
        sql = (
            f'SELECT * FROM {TABLE} '
            f'WHERE {collumn} > %s '
            #  NEVER leave a variable in a sql query, avoid SQL INJECTION
            # f'WHERE id > {id_received} '
        )
        cursor.execute(sql, id_received)
        data5 = cursor.fetchall()

        # for row in data5:
        #     print(row)

    # SELECT Using WHERE and a range of collumn values
    with connection.cursor() as cursor:
        # lowest_id = int(input('Input the lowest ID: '))
        # greatest_id = int(input('Input the greatest ID: '))
        lowest_id = 2
        greatest_id = 4
        sql = (
            f'SELECT * FROM {TABLE} '
            f'WHERE id BETWEEN %s AND %s '
        )
        cursor.execute(sql, (lowest_id, greatest_id))
        # print(cursor.mogrify(sql, (lowest_id, greatest_id)))
        data5 = cursor.fetchall()

        # for row in data5:
        #     print(row)

        # Have no need to commit because there wasn't any change made in READ

    # ------------------------------ DELETE -----------------------------------

    with connection.cursor() as cursor:
        # WARNING - DELETE without WHERE
        # sql = (
        #     f'DELETE FROM {TABLE} '
        # )
        # cursor.execute(sql)
        # connection.commit()

        # cursor.execute(f'SELECT * FROM {TABLE}')
        # for row in cursor.fetchall():
        #     print(row)

        sql = (
            f'DELETE FROM {TABLE} '
            'WHERE id = %s '
        )
        cursor.execute(sql, 2)
        connection.commit()

        cursor.execute(f'SELECT * FROM {TABLE}')
        # for row in cursor.fetchall():
        #     print(row)

    # ------------------------------ UPDATE -----------------------------------

    with connection.cursor() as cursor:
        # WARNING - UPDATE without DELETE
        #     sql = (
        #         f'UPDATE {TABLE} SET name = "YORUICHI" '
        #     )
        #     cursor.execute(sql)
        #     connection.commit()

        #     cursor.execute(f'SELECT * FROM {TABLE}')
        #     for row in cursor.fetchall():
        #         print(row)

        sql = (
            f'UPDATE {TABLE} '
            'SET name = %s, age = %s, unit = %s '
            'WHERE id = %s '
        )
        update_data = ('Minoto', 21, 'Omega', 7)
        cursor.execute(sql, update_data)
        connection.commit()

        cursor.execute(f'SELECT * FROM {TABLE}')
        # for row in cursor.fetchall():
        #     print(row)

    # ------------------------------ UTILITIES --------------------------------

    # Cursor as a DICT (See the connection cursor declaration: LINE 21)
    with connection.cursor() as cursor:
        select_result = cursor.execute(f'SELECT * FROM {TABLE}')

        # for row in cursor.fetchall():
        #     # Get all row values id, name, age, unit
        #     print(row[0])
        #     print(row[1])
        #     print(row[2])
        #     print(row[3])

        # for row in cursor.fetchall():
        #     # Get all row values id, name, age, unit
        #     id, name, age, unit = row
        #     print(id, name, age, unit)

        # for row in cursor.fetchall():
        #     print(row)

        # Cursor scroll (How to return the cursor position)
        # The cursor not stored in any variable is good for performance
        # obviously
        # for row in cursor.fetchall():
        #     print(row)

        # print('\nFor after run out:')
        # cursor.scroll(-2)
        # cursor.scroll(0, 'absolute')'
        # for row in cursor.fetchall():
        #     print(row)

        # SSCursor / SSDict Cursor for great number of data, It returns a
        # generator (You can break the loop and continue later)
        # for row in cursor.fetchall_unbuffered():
        #     print(row)

        #     if row['id'] >= 5:
        #         break

        # print('\nFor after run out:')

        # The generator remembers where he stoped at
        # for row in cursor.fetchall_unbuffered():
        #     print(row)

        # Same result but different ways to achieve
        data6 = cursor.fetchall()

        for row in data6:
            print(row)

        # cursor.execute(f'SELECT id from {TABLE} ORDER BY id DESC LIMIT 1')
        # last_id_from_select = cursor.fetchone()

        print('\nSelect Result', select_result)
        print('len(data6)', len(data6))
        print('rowcount', cursor.rowcount)
        # Last inserted id row in the database, unless it a executemany(), then
        # it returns the first inserted row in the data insert block
        print('lastrowid', cursor.lastrowid)
        # print('lastrowid hardcoded', last_id_from_select)
        print('row number', cursor.rownumber)
