import csv
import random
from datetime import datetime, timedelta


#### BUAT DATA DUMMY 
#### raw data dari sumber data

random.seed(42)


## 1. Create Variations
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam"]
cities = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]
start_date = datetime(2026, 1, 1)
price_range = {
    "Laptop": (8000, 20000),
    "Mouse": (100, 500),
    "Keyboard": (200, 1500),
    "Monitor": (2000, 6000),
    "Headset": (300, 2000),
    "Webcam": (400, 1200),
}


## 2. Write Data
with open("data/raw/sales.csv", "w", newline="") as f:
    writer = csv.writer(f)
    # define columns (1st row)
    writer.writerow([
        "order_id", "customer_id", "product", "city", 
        "amount", "quantity", "order_date"
    ])

    ## create and add 5,000 data
    for i in range(1, 5001):
        ## create 5,000 values for each column
        order_id = i
        cust_id = random.randint(1, 1000)
        product = random.choice(products)
        city = random.choice(cities)
        min_price, max_price = price_range[product]
        amount = round(random.uniform(min_price, max_price), 2)
        quantity = random.randint(1, 5)
        random_days = random.randint(0, 180)
        order_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

        ## add data into row
        writer.writerow([
            order_id,
            cust_id,
            product,
            city,
            amount,
            quantity,
            order_date
        ])

print("Done: 5000 rows written to data/raw/sales.csv")