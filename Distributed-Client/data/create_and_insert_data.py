import random

# 定义表名和列名
table_name = "table1"
columns = ["id", "name", "age", "city"]

# 定义数据生成函数


def generate_data():
    names = ["Alice", "Bob", "Charlie", "David", "Eve",
             "Frank", "Grace", "Heidi", "Ivan", "Judy"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
              "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    return {
        "id": random.randint(1, 100000),
        "name": random.choice(names),
        "age": random.randint(18, 65),
        "city": random.choice(cities)
    }


# 创建表的SQL语句
create_table_sql = f"create table {table_name} (id int, name char(255), age int, city char(255), primary key(id));\n"

# 生成插入数据的SQL语句
insert_data_sql = ""
for _ in range(1000):
    data = generate_data()
    values = [str(data[column]) if isinstance(data[column], int)
              else f"'{data[column]}'" for column in columns]
    insert_data_sql += f"insert into {table_name} values ({', '.join(values)});\n"

# 将SQL语句写入文件
with open("data/cai1.sql", "w") as sql_file:
    sql_file.write(create_table_sql)
    sql_file.write(insert_data_sql)

print("SQL文件已生成：cai1.sql")
