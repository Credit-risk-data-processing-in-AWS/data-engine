import pandas as pd
import random

def generate_data():
    data = []
    for i in range(10000):
        data.append({
            "client_id": f"CL-{random.randint(1000, 2000)}",
            "amount": round(random.uniform(10.0, 5000.0), 2),
            "status": random.choice(["APPROVED", "REJECTED", "PENDING"]),
            "timestamp": pd.Timestamp.now()
        })
    df = pd.DataFrame(data)
    df.to_csv("transactions.csv", index=False)
    print("Dataset 'transactions.csv' creado.")

if __name__ == "__main__":
    generate_data()