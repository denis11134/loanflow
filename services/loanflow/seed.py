import argparse
import json
import os
from pathlib import Path
from secrets import token_urlsafe
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from .store import ROOT, connect, digest, lock_customer, migrate

DEMO = ROOT / "services/scenarios.json"


def seed(path=DEMO, dsn=None):
    data = json.loads(Path(path).read_text())
    tokens = {}
    customers = {}
    with connect(dsn) as db:
        for name, item in data["customers"].items():
            customer_id = UUID(item["customer_id"])
            customers[name] = customer_id
            db.execute("INSERT INTO customer VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                       (customer_id, item.get("payroll", True), item["account_ref"]))
            db.execute("INSERT INTO stand_profile VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                       (customer_id, item["profile_ref"], Jsonb(item.get("scenario", {}))))
        for day in data.get("holidays", []):
            db.execute("INSERT INTO stand_holiday VALUES (%s) ON CONFLICT DO NOTHING", (day,))
        for legacy in sorted(data.get("legacy_contracts", []), key=lambda item: item["customer"]):
            customer_id = customers[legacy["customer"]]
            lock_customer(db, customer_id)
            db.execute("INSERT INTO external_contract_ref VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                       (legacy["external_contract_id"], customer_id, legacy["is_active"]))
        for name, actor in data["actors"].items():
            token = token_urlsafe(32)
            db.execute("""INSERT INTO stand_session
                          (token_digest, principal, role, session_ref, device_ref, customer_id, identified_customer_id)
                          VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                       (digest("bearer", token), name, actor["role"], uuid4(), uuid4(),
                        customers.get(actor.get("customer")), customers.get(actor.get("identified_customer"))))
            tokens[name] = token
        for quota in data.get("quotas", []):
            db.execute("""INSERT INTO mpl_quota VALUES (%s,%s,%s,%s)
                          ON CONFLICT (period_key,bucket_code) DO NOTHING""",
                       (uuid4(), quota["period"], quota["bucket"], quota["capacity"]))
    return tokens


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    load = sub.add_parser("seed")
    load.add_argument("--scenarios", type=Path, default=DEMO)
    load.add_argument("--tokens", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "migrate":
        migrate()
    else:
        with open(args.tokens, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as output:
            try:
                json.dump(seed(args.scenarios), output, indent=2)
            except BaseException:
                args.tokens.unlink()
                raise


if __name__ == "__main__":
    main()
