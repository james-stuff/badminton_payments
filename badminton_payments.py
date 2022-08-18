from pymongo import MongoClient
import arrow
import argparse
import pandas as pd
from io import StringIO
import pathlib


def list_of_names_from_whatsapp(pasted_list: str) -> [str]:
    def extract_name(row: str) -> str:
        if row:
            if "." in row:
                dot_index = row.index(".")
                row = row[dot_index + 1:]
            return row.strip().title()
        return ""

    names = pasted_list.split("\n")
    names = [*filter(lambda x: x, [extract_name(n) for n in names])]
    return names


def create_session(date: arrow.Arrow, people: [str]):
    collection = MongoClient().money.badminton
    session_dt = date.datetime
    date_query = {"Date": {"$eq": session_dt}}
    if collection.find_one(date_query):
        if input(f"Session already exists for {date.format('ddd Do MMMM')}, "
                 f"overwrite? ") in "nN":
            return
        collection.delete_many(date_query)
    document = {name: {} for name in people}
    document["Date"] = session_dt
    collection.insert_one(document)


def multi_line_input(prompt: str) -> str:
    print(prompt)
    sentinel = ""
    return '\n'.join(iter(input, sentinel))


def latest_perse_time() -> arrow.Arrow:
    week = arrow.Arrow.range(frame="days",
                             start=arrow.now(tz="local").shift(days=-7),
                             limit=7)
    time = [*filter(lambda d: d.format("ddd") == "Fri", week)][0]
    return time.floor("day").replace(hour=19, minute=30)


def run_in_add_session_mode():
    raw_names = multi_line_input("Please paste in the list of names from WhatsApp: ")
    name_list = list_of_names_from_whatsapp(raw_names)
    create_session(latest_perse_time(), name_list)


def account_mappings_from_raw(raw: str) -> dict:
    mappings = {}
    for row in raw.split("\n"):
        k, v = (m for m in row.split("\t"))
        mappings[k] = v
    return mappings


def migrate_mappings_to_mongo(raw_mappings: str):
    record = {"_id": "AccountMappings"}
    collection = MongoClient().money.badminton
    if not collection.find_one(record):
        collection.insert_one(record)
    collection.update_one(record, {"$set": account_mappings_from_raw(raw_mappings)})


def create_nationwide_dataset(passed_data: str = "",
                              session_date = None) -> pd.DataFrame:
    csv_input = StringIO(passed_data)
    kw_args = {"names": ["Date", "Account ID", "AC Num", "Blank", "Value", "Balance"]}
    if passed_data:
        kw_args["delimiter"] = "\t"
    else:
        csv_input = get_latest_nationwide_csv_filename()
        kw_args["encoding"] = "cp1252"
        kw_args["skiprows"] = 5
    bank_df = pd.read_csv(csv_input, **kw_args)
    bank_df = clean_nationwide_data(bank_df, session_date)
    print(bank_df)
    return bank_df


def clean_nationwide_data(df_bank: pd.DataFrame, session_date = None) -> pd.DataFrame:
    """assumes that earliest payments are received the day after the session"""
    next_saturday = arrow.get(session_date).shift(days=8).date()
    df_bank["Date"] = pd.to_datetime(df_bank["Date"])
    df_bank["Account ID"] = df_bank["Account ID"].str[12:]
    df_bank.loc[df_bank["Account ID"] == "m", "Account ID"] = df_bank["AC Num"].str[:15]
    money_fields = ["Value", "Balance"]
    for mf in money_fields:
        df_bank[mf] = pd.to_numeric(df_bank[mf].str.strip("£"))
    df_bank = df_bank.drop(["AC Num", "Blank"], axis=1)
    df_out = df_bank.loc[(df_bank["Date"] > pd.Timestamp(session_date)) &
                         (df_bank["Date"] < pd.Timestamp(next_saturday))]
    print(df_out.info())
    # TODO: remove payments to me (AC Num field)
    # TODO: second Mohan Krishna payment for Kelsey Kerridge makes things interesting!
    return df_out


def get_latest_nationwide_csv_filename() -> str:
    downloads_folder = pathlib.Path("C:\\Users\\j_a_c\\Downloads")
    file_listing = downloads_folder.glob("Statement Download*.csv")
    if file_listing:
        return str(max(file_listing, key=lambda file: file.stat().st_ctime))
    return ""


def monday_process(pasted_text: str = "") -> None:
    print('')

    coll = MongoClient().money.badminton
    mappings = coll.find_one({"_id": "AccountMappings"})
    session = coll.find_one({"Date": {"$eq": latest_perse_time().datetime}})

    # per_person_cost = float(input("Please enter the amount charged per person: £"))
    per_person_cost = 4.4
    me = "James (Host)"
    if me in session:
        record_payment(me, per_person_cost, "host")

    def get_new_alias_from_input(account_name: str, not_paid: [str]) -> str:
        initials = [word[0] for word in account_name.split()]
        right_initials = [name for name in not_paid if name[0] in initials]
        shortlist = sorted(right_initials,
                           key=lambda s: initials.index(s[0]))
        keyed = input(f"Who is {account_name}?\n{choice_of_names(shortlist)}\n")
        if keyed.isnumeric():
            return shortlist[int(keyed) - 1]
        keyed = input(f"Who is {account_name}?\n{choice_of_names(not_paid)}\n")
        return not_paid[int(keyed) - 1]

    bank_df = create_nationwide_dataset(pasted_text, session["Date"])
    # TODO: what if there are two payments from the same account?
    for df_index in bank_df.index:
        account_id = bank_df.loc[df_index]["Account ID"]
        payment_amount = bank_df.loc[df_index]["Value"]
        if account_id in mappings:
            alias = mappings[account_id]
            unpaid = get_unpaid()
            if isinstance(alias, list):
                valid_aliases = [*filter(lambda name: name in session, alias)]
                alias = ""
                if valid_aliases:
                    alias = valid_aliases[0]
            if alias in unpaid:
                record_payment(alias, payment_amount)
            elif alias not in session:
                """Steve L, Ali I: previous alias is not in current session"""
                # TODO: what if one of their other aliases (not [0]) is?
                new_alias = get_new_alias_from_input(f"{alias}/{account_id}", unpaid)
                aliases = [alias] if isinstance(alias, str) else alias
                aliases.append(new_alias)
                set_new_alias(account_id, aliases)
                record_payment(new_alias, payment_amount)
        else:
            """previously un-encountered account id"""
            unpaid = get_unpaid()
            attendee = get_new_alias_from_input(account_id, unpaid)
            set_new_alias(account_id, attendee)
            record_payment(attendee, payment_amount)

    while input("Did anyone pay in cash? ") in "yY":
        unpaid = get_unpaid()
        choice = int(input(f"Who?\n{choice_of_names(unpaid)}\n"))
        cash_amount = float(input(f"How much did they pay? £"))
        # print(f"You are telling me {unpaid[choice]} has paid £{amount}")
        record_payment(unpaid[choice - 1], cash_amount, "cash")

    while input("Were there any no-shows? ") in "yY":
        unpaid = get_unpaid()
        choice = int(input(f"Who?\n{choice_of_names(unpaid)}\n"))
        record_payment(unpaid[choice - 1], 0, "no show")

    after = coll.find_one({"Date": {"$eq": latest_perse_time().datetime}})
    print(f"Found session:\n{after}")
    still_unpaid = get_unpaid()
    if still_unpaid:
        print(f"{still_unpaid} have not paid.  That is {len(still_unpaid)} people.")
    payments_string = "\n".join([f"\t£{get_total_payments(after, t):.2f} in {t}"
                                 for t in ("transfer", "host", "cash")])
    print(f"So far have received \n{payments_string}\nfor this session.")


def set_new_alias(account_name: str, alias: object):
    """alias can be string or list of strings"""
    coll = MongoClient().money.badminton
    coll.update_one({"_id": "AccountMappings"},
                    {"$set": {account_name: alias}})


def choice_of_names(names: [str]) -> str:
    option_list = [f"[{i + 1}] {name}" for i, name in enumerate(names)] + \
                  ["[?] Don't know"]
    display_string = ""
    max_line_length = 80
    while option_list:
        next_line = "\t"
        for index, text in enumerate(option_list):
            if len(next_line) + len(text) <= max_line_length:
                next_line += f"\t{text}"
                if index == len(option_list) - 1:
                    display_string += next_line
                    option_list = []
            else:
                next_line += "\n"
                display_string += next_line
                option_list = option_list[index:]
                break
    return display_string


def record_payment(attendee: str, amount: float, payment_type: str = "transfer"):
    coll = MongoClient().money.badminton
    coll.update_one({"Date": {"$eq": latest_perse_time().datetime}},
                    {"$set": {attendee: {payment_type: amount}}})
    print(f"{payment_type} transaction of £{amount} added for {attendee}")


def get_unpaid() -> [str]:
    coll = MongoClient().money.badminton
    session = coll.find_one({"Date": {"$eq": latest_perse_time().datetime}})
    return [k for k in session if k not in ["Date", "_id"] and not session[k]]


def get_total_payments(session: dict, payment_type: str = "transfer") -> float:
    return sum([v[payment_type] for v in session.values()
                if isinstance(v, dict) and payment_type in v])


if __name__ == "__main__":
    my_parser = argparse.ArgumentParser(description='Badminton payments processing')
    my_parser.add_argument('Operation',
                           metavar='operation',
                           type=str,
                           help='either [F] set up a new session or [M] process payments for existing session')
    args = my_parser.parse_args()
    op = args.Operation
    if op == "F":
        run_in_add_session_mode()
    elif op == "M":
        monday_process("")
    else:
        print(f"{op} is not a valid operation code")

