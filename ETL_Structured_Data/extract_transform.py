import pandas as pd
from sqlalchemy import create_engine

def extract_transform():
    print("=== Extracting and Transforming data ===")

    # Koneksi
    source_engine = create_engine("postgresql+psycopg2://postgres:dataEngginer@localhost:5432/adventureworks")
    staging_engine = create_engine("postgresql+psycopg2://postgres:dataEngginer@localhost:5432/staggingDB")

    # === Extract ===
    tables = {
        "customer": "sales.customer",
        "person": "person.person",
        "product": "production.product",
        "productcategory": "production.productcategory",
        "productsubcategory": "production.productsubcategory",
        "salesorderheader": "sales.salesorderheader",
        "salesorderdetail": "sales.salesorderdetail",
        "salesterritory": "sales.salesterritory",
        "stateprovince": "person.stateprovince",
        "countryregion": "person.countryregion"
    }

    for name, source_table in tables.items():
        print(f"Mengambil dan menyimpan tabel: {name}")
        df = pd.read_sql(f"SELECT * FROM {source_table}", source_engine)
        df.to_sql(name, staging_engine, schema='public', if_exists='replace', index=False)

    print("Extraction selesai. Data mentah disalin ke staging.")

    # === Transform ===
    df_customer = pd.read_sql("""
        SELECT c.customerid,
               pp.firstname || ' ' || COALESCE(pp.middlename || ' ', '') || pp.lastname AS customername
        FROM customer c
        JOIN person pp ON c.personid = pp.businessentityid;
    """, staging_engine)
    df_customer["customer_key"] = range(1, len(df_customer) + 1)

    df_product = pd.read_sql("""
        SELECT p.productid, pc.name AS productsubcategory, p.name AS productname
        FROM product p
        JOIN productsubcategory psc ON p.productsubcategoryid = psc.productsubcategoryid
        JOIN productcategory pc ON psc.productcategoryid = pc.productcategoryid;
    """, staging_engine)
    df_product["product_key"] = range(1, len(df_product) + 1)

    df_territory = pd.read_sql("""
        SELECT st.territoryid, sp.name AS provincename, cr.name AS countryregion
        FROM salesterritory st
        JOIN stateprovince sp ON st.territoryID = sp.territoryID
        JOIN countryregion cr ON st.countryregioncode = cr.countryregioncode;
    """, staging_engine)
    df_territory["territory_key"] = range(1, len(df_territory) + 1)

    df_sales = pd.read_sql("""
        SELECT soh.orderdate, soh.customerid, soh.territoryid, sod.productid,
               sod.orderqty, soh.totaldue
        FROM salesorderdetail sod
        JOIN salesorderheader soh ON sod.salesorderid = soh.salesorderid;
    """, staging_engine)

    df_time = df_sales[['orderdate']].drop_duplicates()
    df_time["year"] = pd.to_datetime(df_time["orderdate"]).dt.year
    df_time["month"] = pd.to_datetime(df_time["orderdate"]).dt.month
    df_time["day"] = pd.to_datetime(df_time["orderdate"]).dt.day
    df_time["time_key"] = range(1, len(df_time) + 1)

    df_sales = df_sales.merge(df_customer[["customerid", "customer_key"]], on="customerid", how="left")
    df_sales = df_sales.merge(df_product[["productid", "product_key"]], on="productid", how="left")
    df_sales = df_sales.merge(df_territory[["territoryid", "territory_key"]], on="territoryid", how="left")
    df_sales = df_sales.merge(df_time[["orderdate", "time_key"]], on="orderdate", how="left")

    df_fact = df_sales.groupby(["customer_key", "product_key", "territory_key", "time_key"]).agg(
        total_quantity=("orderqty", "sum"),
        average_amount=("totaldue", "mean"),
        total_revenue=("totaldue", "sum")
    ).reset_index()
    df_fact["sales_id"] = range(1, len(df_fact) + 1)

    print("Transformasi selesai.")
    return df_customer, df_product, df_territory, df_time, df_fact
