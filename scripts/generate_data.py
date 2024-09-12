from pyspark.sql import SparkSession
from faker import Faker
import random
import concurrent.futures

# Initialize PySpark session
spark = SparkSession.builder.appName(
    "PySpark SQL Server Bulk Insert OLTP and OLAP"
).getOrCreate()

# JDBC connection properties for OLTP_DB and OLAP_DB
server = "localhost"
port = "1433"
oltp_db = "OLTP_DB"
olap_db = "OLAP_DB"
url_oltp = f"jdbc:sqlserver://{server}:{port};databaseName={oltp_db}"
url_olap = f"jdbc:sqlserver://{server}:{port};databaseName={olap_db}"
properties = {
    "user": "sa",
    "password": "YourStrong!Passw0rd",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Initialize Faker for generating fake data
fake = Faker()


# Function to insert data into SQL Server using PySpark
def bulk_insert(df, table_name, url):
    df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)


# Generate bulk data for Customers table
def generate_customers_data(num_records):
    data = []
    for _ in range(num_records):
        data.append(
            (
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.date_between(start_date="-5y", end_date="today"),
            )
        )
    return data


# Generate bulk data for Products table
def generate_products_data(num_records):
    data = []
    for _ in range(num_records):
        data.append((fake.word().capitalize(), round(random.uniform(5, 500), 2)))
    return data


# Generate bulk data for Orders table
def generate_orders_data(num_records):
    statuses = ["Shipped", "Pending", "Cancelled"]
    data = []
    for _ in range(num_records):
        data.append(
            (
                random.randint(1, 1000000),
                fake.date_between(start_date="-5y", end_date="today"),
                random.choice(statuses),
            )
        )
    return data


# Generate bulk data for OrderDetails table
def generate_order_details_data(num_records):
    data = []
    for _ in range(num_records):
        data.append(
            (
                random.randint(1, 2000000),
                random.randint(1, 10000),
                random.randint(1, 10),
                round(random.uniform(5, 500), 2),
            )
        )
    return data


# Generate bulk data for Sales table
def generate_sales_data(num_records):
    data = []
    for _ in range(num_records):
        data.append(
            (
                random.randint(1, 2000000),
                fake.date_between(start_date="-5y", end_date="today"),
                round(random.uniform(50, 5000), 2),
            )
        )
    return data


# Function to insert into OLTP_DB
def insert_into_oltp_db():
    # Insert into Customers
    customers_data = generate_customers_data(1000000)  # 1 million customers
    df_customers = spark.createDataFrame(
        customers_data, ["FirstName", "LastName", "Email", "JoinDate"]
    )
    bulk_insert(df_customers, "dbo.Customers", url_oltp)

    # Insert into Products
    products_data = generate_products_data(10000)  # 10k products
    df_products = spark.createDataFrame(products_data, ["ProductName", "Price"])
    bulk_insert(df_products, "dbo.Products", url_oltp)

    # Insert into Orders
    orders_data = generate_orders_data(2000000)  # 2 million orders
    df_orders = spark.createDataFrame(
        orders_data, ["CustomerID", "OrderDate", "Status"]
    )
    bulk_insert(df_orders, "dbo.Orders", url_oltp)

    # Insert into OrderDetails
    order_details_data = generate_order_details_data(5000000)  # 5 million order details
    df_order_details = spark.createDataFrame(
        order_details_data, ["OrderID", "ProductID", "Quantity", "Price"]
    )
    bulk_insert(df_order_details, "dbo.OrderDetails", url_oltp)

    # Insert into Sales
    sales_data = generate_sales_data(1000000)  # 1 million sales
    df_sales = spark.createDataFrame(sales_data, ["OrderID", "SaleDate", "TotalAmount"])
    bulk_insert(df_sales, "dbo.Sales", url_oltp)


# Function to insert into OLAP_DB
def insert_into_olap_db():
    # Insert summarized data into OLAP_DB
    sales_summary_data = [
        (
            fake.word().capitalize(),
            random.randint(100, 10000),
            round(random.uniform(1000, 100000), 2),
        )
        for _ in range(10000)  # Example of 10,000 summary records for OLAP
    ]
    df_sales_summary = spark.createDataFrame(
        sales_summary_data, ["ProductName", "TotalQuantity", "TotalSales"]
    )
    bulk_insert(df_sales_summary, "dbo.SalesSummary", url_olap)


# Function to insert into both OLTP and OLAP databases concurrently
def insert_into_both_oltp_and_olap():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_oltp = executor.submit(insert_into_oltp_db)
        future_olap = executor.submit(insert_into_olap_db)
        concurrent.futures.wait([future_oltp, future_olap])


# Main execution
if __name__ == "__main__":
    # Insert data into OLTP_DB and OLAP_DB concurrently
    insert_into_both_oltp_and_olap()

    # Stop the Spark session
    spark.stop()
