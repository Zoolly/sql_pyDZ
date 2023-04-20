import psycopg2
from conf import USER, PASSWORD, HOST, PORT
import os


def create_db(name: str, user):
    os.system(f'createdb -U {user} {name}')


def delete_table(connection, cursor, *names_table):
    for name in names_table:
        cursor.execute(f"""
                DROP TABLE IF EXISTS {name} CASCADE;
            """)
        connection.commit()
        print(f'(INFO): Table: {name} is deleted')


def create_table(connection, cursor, name_table: str, **table_columns):
    cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {name_table} ({name_table}_id SERIAL PRIMARY KEY);                                            
        """)
    connection.commit()
    for name_column, type_column in table_columns.items():
        cursor.execute(f"""
                ALTER TABLE {name_table} ADD COLUMN {name_column} {type_column}        
                        """)
        connection.commit()
    print(f'(INFO): Table: {name_table} is created')


def add_client(connection, cursor, name_table, **values):
    cursor.execute(f"""
        INSERT INTO {name_table}({", ".join(key for key in values.keys())})
        VALUES ({", ".join(item for item in values.values())})
    """)
    connection.commit()
    cursor.execute(f"""
        SELECT {name_table}_id FROM {name_table}
    """)
    print(f'(INFO): adding successful: {name_table}_id is {cursor.fetchone()[0]}')


def add_phone(connection, cursor, name_table, client_id, phone: str):
    cursor.execute(f"""
        INSERT INTO {name_table} (client_id, phone)
        VALUES ({client_id}, '{phone}');
    """)
    connection.commit()
    cursor.execute(f"""
        SELECT first_name, last_name, phone 
        FROM {name_table}
            INNER JOIN client USING (client_id);
    """)
    result = [" ".join(item).split() for item in cursor]
    print(f"(INFO): adding successful: phone added for {result[0][0]} {result[0][1]} phone is {result[0][2]}.")


def change_client(connection, cursor, name_table, client_id, **values):
    for key, value in values.items():
        cursor.execute(f"""
            UPDATE {name_table}
            SET {key} = {value}
            WHERE client_id = {client_id}
        """)
        connection.commit()
    cursor.execute(f"""
        SELECT * FROM {name_table} WHERE client_id = {client_id}
    """)
    print(f'(INFO): change successful: client update {cursor.fetchone()}')


def del_phone(connection, cursor, name_table, client_id, phone):
    cursor.execute(f"""
        DELETE FROM {name_table}
        WHERE client_id = {client_id} AND phone = {phone}
    """)
    connection.commit()
    print(f"(INFO): delete successful")


def del_client(connection, cursor, name_table, client_id):
    cursor.execute(f"""
        DELETE FROM {name_table}
        WHERE client_id = {client_id}
    """)
    connection.commit()
    print(f"(INFO): delete successful")


def find_client(cursor, **values):
    for key, value in values.items():
        cursor.execute(f"""
            SELECT client_id, first_name, last_name, email, phone
            FROM client INNER JOIN phone USING (client_id)
            WHERE {key} = '{value}'
        """)
        result = cursor.fetchall()
        if result:
            print(f'(INFO): found successful: {result}')
        else:
            print(f'(INFO): error: client not found')


if __name__ == '__main__':
    create_db('clients', USER)
    conn = psycopg2.connect(dbname='clients', user=USER, password=PASSWORD, host=HOST, port=PORT)
    try:
        with conn:
            with conn.cursor() as cur:
                delete_table(conn, cur, 'client', 'phone')
                create_table(conn, cur, 'client', first_name='VARCHAR(30)', last_name='VARCHAR(30)', email='VARCHAR(60)')
                create_table(conn, cur, 'phone', client_id='INTEGER REFERENCES client (client_id)', phone='VARCHAR(20)')
                add_client(conn, cur, 'client', first_name="'Ivan'", last_name="'Ivanov'", email="'email@test.com'")
                add_phone(conn, cur, 'phone', 1, '89251234567')
                change_client(conn, cur, 'client', 1, first_name="'Petr'", last_name="'Petrov'")
                del_phone(conn, cur, 'phone', 1, "'89251234567'")
                del_client(conn, cur, 'client', 1)
                find_client(cur, phone='89251234567')
    finally:
        conn.close()