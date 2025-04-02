import requests as req
from typing import NamedTuple
import pymssql
import datetime as dt
import re
from rich.console import Console


PIPELINE_ID = 16
FILTER_ID = 243
PIPEDRIVE_URL = "https://api.pipedrive.com/v1"
PIPEDRIVE_KEY = "4e63fbb2706f7b20fe4de85fcbea5cc476cce631"


DB_SERVER = "129.159.61.143"
DB_USER = "caburepro"
DB_PASSWORD = "Tw55*%2018,()&CurtX"
DB_NAME = "CABURE_PRD"
DB_PORT = "55098"


class Deal(NamedTuple):
    id: int
    name: str
    email: str
    phone: str
    owner_name: str


class Sale(NamedTuple):
    proposal_id: str
    product_id: int
    premium: float
    sale_date: dt.datetime
    seller_phone: str
    pipe_product_id: int
    insured_id: int


def normalize_phone_number(phone: str):
    pattern = re.compile(r"(?:\+55)?\(?(\d{2})\)?[ ]?(\d{5})-?(\d{4})")
    return re.sub(pattern=pattern, repl=r"\1\2\3", string=phone)


def get_already_synced() -> list[str]:
    with open("already_synced.txt") as fp:
        ids = [line.strip() for line in fp.readlines()]
    return ids


def normalize_product_id(product_id: int) -> int:
    normalizer = {
        100001: 2,
        100002: 3,
        100003: 4,
    }

    return normalizer.get(product_id, 999999999)


def get_sales_from_db(
    conn: pymssql.Connection, beg_date: dt.date, end_date: dt.date
) -> list[Sale]:
    fmtstr = "%Y-%m-%d"

    query = f"""
    SELECT
        ssp.ID_PROPOSTA as 'proposal_id',
        ssp.ID_TABELA as 'product_id',
        ssp.TOTAL_PREMIO_FINAL as 'premium',
        ssp.DATA_CADASTRO as 'sale_date',
        sa.CELULAR as 'seller_phone',
        ssp.ID_SEGURADO as 'insured_id'
    FROM SEG_SEGURO_PROPOSTA ssp
    JOIN SEG_AGENCIADOR sa on sa.ID_AGENCIADOR = ssp.ID_AGENCIADOR
    WHERE ssp.DATA_CADASTRO BETWEEN '{beg_date.strftime(fmtstr)}' AND '{end_date.strftime(fmtstr)}'
        AND ssp.ID_TABELA in (100001, 100002, 100003)
        AND ssp.ID_SITUACAO NOT IN (3,13)
        AND (sa.ID_MENTOR IS null OR sa.ID_MENTOR NOT IN (2,4,5))"""

    cur = conn.cursor()
    cur.execute(query)

    sales = [
        Sale(
            proposal_id=s[0],
            product_id=int(s[1]),
            premium=float(s[2]),
            sale_date=s[3],
            seller_phone=normalize_phone_number(s[4]),
            pipe_product_id=normalize_product_id(int(s[1])),
            insured_id=int(s[5]),
        )
        for s in cur
        if s
    ]
    return sales


def get_deals_from_pipedrive() -> dict[str, Deal]:
    endpoint_url = PIPEDRIVE_URL + "/deals"
    params = {"api_token": PIPEDRIVE_KEY, "filter_id": FILTER_ID}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    res = req.get(endpoint_url, params=params, headers=headers)

    deals: dict[str, Deal] = {}
    while res:
        res = res.json()

        for json_deal in res["data"]:
            if json_deal["person_id"]["phone"][0]["value"]:
                deal = Deal(
                    id=json_deal["id"],
                    name=json_deal["person_id"]["name"],
                    email=json_deal["person_id"]["email"][0]["value"],
                    phone=json_deal["person_id"]["phone"][0]["value"],
                    owner_name=json_deal["owner_name"],
                )
                deals[normalize_phone_number(deal.phone)] = deal

        pagination_info = res["additional_data"]["pagination"]
        if pagination_info["more_items_in_collection"]:
            res = req.get(
                endpoint_url, params={**params, "start": pagination_info["next_start"]}
            )
        else:
            res = None
    return deals


def append_sales_to_deal_as_products(deal_id: int, sales: list[Sale]) -> bool:
    endpoint_url = f"{PIPEDRIVE_URL}/deals/{deal_id}/products"
    params = {"api_token": PIPEDRIVE_KEY}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    for sale in sales:
        data = {
            "product_id": sale.pipe_product_id,
            "item_price": sale.premium,
            "quantity": 1,
        }
        res = req.post(endpoint_url, params=params, headers=headers, json=data)
        if res.status_code != 201:
            print(res.__dict__)
            return False
    return True


def append_sales_to_deal_as_activities(deal_id: int, sales: list[Sale]) -> bool:
    endpoint_url = f"{PIPEDRIVE_URL}/activities"
    params = {"api_token": PIPEDRIVE_KEY}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    for sale in sales:
        data = {
            "due_date": sale.sale_date.strftime("%Y-%m-%d"),
            "deal_id": deal_id,
            "done": 1,
            "type": "vf___venda_feita",
            "subject": f"Venda - Segurado {sale.insured_id}",
        }
        res = req.post(endpoint_url, params=params, headers=headers, json=data)
        if res.status_code != 201:
            print(res.__dict__)
            return False
    return True


def main() -> None:
    console = Console()

    conn = pymssql.connect(
        server=DB_SERVER,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        encryption=None,
        read_only=True,
    )

    deals_in_pipeline = get_deals_from_pipedrive()
    already_synced = get_already_synced()

    beg_date = dt.date(2024, 5, 1)  # inclusive
    end_date = dt.date(2025, 3, 31)  # not inclusive
    sales = get_sales_from_db(conn, beg_date, end_date)

    sales_by_deal_id: dict[int, list[Sale]] = {}
    for sale in sales:
        if sale.seller_phone not in deals_in_pipeline:
            console.print(
                f"ERRO: Telefone {sale.seller_phone} não corresponde a nenhum negócio no Pipedrive",
                style="yellow",
            )
            continue
        elif sale.proposal_id in already_synced:
            console.print(f"SKIP: Venda {sale.proposal_id} já incluída, pulando")
            continue

        deal_id = deals_in_pipeline[sale.seller_phone].id

        if deal_id in sales_by_deal_id:
            sales_by_deal_id[deal_id].append(sale)
        else:
            sales_by_deal_id[deal_id] = [sale]

    fp = open("already_synced.txt", "a+")

    for deal_id, sales in sales_by_deal_id.items():
        console.print(
            f"Incluindo vendas do negócio {deal_id} ({len(sales)} vendas)",
            style="green",
        )
        if append_sales_to_deal_as_activities(deal_id, sales):
            fp.writelines([f"{s.proposal_id}\n" for s in sales])
        else:
            print(
                f"ERRO: ID PIPE - {deal_id} PROPOSTAS - {[s.proposal_id for s in sales]}"
            )

    fp.close()
    conn.close()
    return


if __name__ == "__main__":
    main()
