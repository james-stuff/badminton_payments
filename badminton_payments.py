from pymongo import MongoClient
import arrow
import argparse
import pandas as pd
from io import StringIO


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


def monday_process(data: str) -> None:
    # TODO: import .csv file from statement download
    #       assume the first transfer doesn't arrive until Saturday
    #       (have never seen any on same day, and Kevin K is often a week late!)
    print('')
    bank_df = pd.read_csv(StringIO(data), "\t", names=["Date", "Name", "AC Num",
                                                       "Blank", "Value", "Balance"])
    money_fields = ["Value", "Balance"]
    for mf in money_fields:
        bank_df[mf] = pd.to_numeric(bank_df[mf].str.strip("£"))
    bank_df["Date"] = pd.to_datetime(bank_df["Date"])
    bank_df = bank_df.drop(["Blank"], axis=1)
    print(bank_df)

    coll = MongoClient().money.badminton
    mappings = coll.find_one({"_id": "AccountMappings"})
    session = coll.find_one({"Date": {"$eq": latest_perse_time().datetime}})

    # per_person_cost = float(input("Please enter the amount charged per person: £"))
    per_person_cost = 4.4
    me = "James (Host)"
    if me in session:
        record_payment(me, per_person_cost, "host")

    for df_index in range(len(bank_df)):
        ac_name = bank_df.iloc[df_index]["Name"][12:]
        ac_number = bank_df.iloc[df_index]["AC Num"][:15]
        if ac_name in mappings:
            alias = mappings[ac_name]
            unpaid = get_unpaid()
            if isinstance(alias, list):
                valid_aliases = [*filter(lambda name: name in session, alias)]
                if valid_aliases:
                    alias = valid_aliases[0]
                else:
                    alias = ""
            if alias in unpaid:
                record_payment(alias, bank_df.iloc[df_index]['Value'])
            elif alias not in session:
                choice = input(f"Who is {mappings[ac_name]}?\n"
                               f"{choice_of_names(unpaid)}\n")
                new_alias = unpaid[int(choice)]
                aliases = [alias] if isinstance(alias, str) else alias
                aliases.append(new_alias)
                coll.update_one({"_id": "AccountMappings"},
                                {"$set": {ac_name: aliases}})
                record_payment(new_alias, bank_df.iloc[df_index]['Value'])
        elif ac_number in mappings:
            if mappings[ac_number] in get_unpaid():
                record_payment(mappings[ac_number], bank_df.iloc[df_index]['Value'])
            elif mappings[ac_number] not in session:
                print(f"Who is {mappings[ac_number]}?")
        else:
            unpaid = get_unpaid()
            initial = ac_name[0]
            possibles = [p for p in unpaid if p[0] == initial]
            choice = input(f"Who is {ac_name}?\n"
                           f"{choice_of_names(possibles)}\n")
            if choice.isnumeric():
                attendee = possibles[int(choice)]
                coll.update_one({"_id": "AccountMappings"},
                                {"$set": {ac_name: attendee}})
                record_payment(attendee, bank_df.iloc[df_index]['Value'])
            else:
                choice = input(f"Choose from all unpaid attendees:\n"
                               f"{choice_of_names(unpaid)}\n")
                if choice.isnumeric():
                    attendee = unpaid[int(choice)]
                    print(f"After choosing ?, you picked {attendee}")
                    coll.update_one({"_id": "AccountMappings"},
                                    {"$set": {ac_name: attendee}})
                    record_payment(attendee, bank_df.iloc[df_index]['Value'])

    while input("Did anyone pay in cash? ") in "yY":
        unpaid = get_unpaid()
        choice = int(input(f"Who?\n{choice_of_names(unpaid)}\n"))
        amount = float(input(f"How much did they pay? £"))
        # print(f"You are telling me {unpaid[choice]} has paid £{amount}")
        record_payment(unpaid[choice], amount, "cash")

    while input("Were there any no-shows? ") in "yY":
        unpaid = get_unpaid()
        choice = int(input(f"Who?\n{choice_of_names(unpaid)}\n"))
        record_payment(unpaid[choice], 0, "no show")

    after = coll.find_one({"Date": {"$eq": latest_perse_time().datetime}})
    print(f"Found session:\n{after}")
    still_unpaid = get_unpaid()
    print(f"{still_unpaid} have not paid.  That is {len(still_unpaid)} people.")
    payments_string = "\n".join([f"\t£{get_total_payments(after, t):.2f} in {t}"
                                 for t in ("transfer", "host", "cash")])
    print(f"So far have received \n{payments_string}\nfor this session.")


def choice_of_names(names: [str]) -> str:
    option_list = [f"[{i}] {name}" for i, name in enumerate(names)] + ["[?] Don't know"]
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

