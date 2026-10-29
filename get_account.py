import os
from dotenv import load_dotenv
from t_tech.invest import Client
from t_tech.invest.constants import INVEST_GRPC_API_SANDBOX

load_dotenv()
TOKEN = os.getenv("TINKOFF_SANDBOX_TOKEN")

with Client(TOKEN, target=INVEST_GRPC_API_SANDBOX) as client:
    accounts = client.users.get_accounts().accounts
    print(f"Найдено счетов: {len(accounts)}")
    for acc in accounts:
        print(f"  ID: {acc.id} | Статус: {acc.status} | Название: {acc.name}")