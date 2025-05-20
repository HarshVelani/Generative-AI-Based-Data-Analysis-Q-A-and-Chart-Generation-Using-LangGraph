import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Mysqlharsh17@",
    database="adventureworksdb",
    connection_timeout=5,
    connect_timeout=5,
    use_pure=True
)
print(f"Connected to database 'adventureworksdb'")

cursor = conn.cursor(dictionary=True)
cursor.execute("""SELECT T2.SubcategoryName, AVG(T1.ProductCost) as average_product_cost 
FROM product T1 
JOIN product_subcategory T2 
ON T1.ProductSubcategoryKey = T2.ProductSubcategoryKey 
GROUP BY T2.SubcategoryName 
ORDER BY average_product_cost DESC""")

sql_results = cursor.fetchall()
conn.close()
print("Connection closed.")

data = pd.DataFrame(sql_results)

plt.figure(figsize=(10,6))
plt.bar(data['SubcategoryName'], data['average_product_cost'], color ='maroon')
plt.xlabel("Subcategory Name")
plt.ylabel("Average Product Cost")
plt.title("Average Product Cost by Subcategory")
plt.legend()
plt.xticks(rotation=90)

dynamic_name = 'bar_' + 'average_product_cost'
plt.savefig(dynamic_name + '.png', bbox_inches='tight')
print("=======> Chart Generated")