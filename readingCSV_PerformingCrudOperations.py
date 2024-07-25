import pandas as pd
import mysql.connector
from mysql.connector import Error

df=pd.read_csv('employeesData.csv')
employee_data_list=[]
for n,item in df.iterrows():
    employee_data_list.append(item.to_list())


try:
    conn=mysql.connector.connect(
        host='localhost',
        user='root',
        password='#######',
        database='testdb'
    )
    if conn.is_connected():
        print(f'My SQL server Connected')

    cursor=conn.cursor()
    table_name='employees'
    insert_query=f"""
    INSERT INTO {table_name} (id, name, dept, salary) 
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    dept = VALUES(dept),
    salary = VALUES(salary)
"""
    cursor.executemany(insert_query,employee_data_list)
    print(f'{cursor.rowcount} Rows Inserted')
    conn.commit()

    select_query=f"""
    Select * from {table_name}
    """
    cursor.execute(select_query)
    print(item[0]for item in cursor.description)
    print(cursor.fetchall())


except Error as e:
    print(f'Error is {e}')
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Connection is Closed.")

