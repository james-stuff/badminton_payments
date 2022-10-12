from pymongo import MongoClient
import arrow
import argparse
import pandas as pd
from io import StringIO
import pathlib


def set_session_date(new_date: arrow.Arrow):
    global session_date
    session_date = new_date


def ensure_uniqueness(names: [str]) -> [str]:
    """where duplicate names are encountered, appends _n"""
    if len(set(names)) == len(names):
        return names
    unique_names, name_counters = [], {}
    for nm in names:
        if nm not in unique_names:
            unique_names.append(nm)
        else:
            counter = 1
            if nm in name_counters:
                counter = name_counters[nm] + 1
            unique_names.append(f"{nm}_{counter}")
            name_counters[nm] = counter
    return unique_names


def list_of_names_from_whatsapp(pasted_list: str) -> [str]:
    def extract_name(row: str) -> str:
        if row:
            if "." in row:
                dot_index = row.index(".")
                row = row[dot_index + 1:]
            return row.replace(".", "").strip().title()
        return ""

    names = pasted_list.split("\n")
    names = [*filter(lambda x: x, [extract_name(n) for n in names])]
    names = ensure_uniqueness(names)
    return names


def create_session(people: [str]):
    collection = MongoClient().money.badminton
    mongo_date = session_date.datetime
    date_query = {"Date": {"$eq": mongo_date}}
    if collection.find_one(date_query):
        if input(f"Session already exists for {session_date.format('ddd Do MMMM')}, "
                 f"overwrite? ") in "nN":
            return
        collection.delete_many(date_query)
    document = {"People": {name: {} for name in people}}
    document["Date"] = mongo_date
    collection.insert_one(document)


def multi_line_input(prompt: str) -> str:
    print(prompt)
    sentinel = ""
    return '\n'.join(iter(input, sentinel))


def get_latest_perse_time(request_time: arrow.Arrow = arrow.now(tz="local")) -> arrow.Arrow:
    week = arrow.Arrow.range(frame="days",
                             start=request_time.shift(days=-7),
                             limit=7)
    time = [*filter(lambda d: d.format("ddd") == "Fri", week)][0]
    return time.floor("day").replace(hour=19, minute=30)


def time_machine(requested_date: arrow.Arrow) -> arrow.Arrow:
    """Get time of most recent Perse session for a given date.
    If a Friday is requested, it will return session time on the same day"""
    return get_latest_perse_time(requested_date.shift(days=1).to("local"))


def run_in_add_session_mode(names: str = ""):
    if names:
        raw_names = names
    else:
        raw_names = multi_line_input("Please paste in the list of names from WhatsApp: ")
    name_list = list_of_names_from_whatsapp(raw_names)
    create_session(name_list)


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


def create_nationwide_dataset(passed_data: str = "") -> pd.DataFrame:
    csv_input = StringIO(passed_data)
    kw_args = {"names": ["Date", "Account ID", "AC Num", "Blank", "Value", "Balance"]}
    if passed_data:
        kw_args["delimiter"] = "\t"
    else:
        csv_input = get_latest_nationwide_csv_filename()
        kw_args["encoding"] = "cp1252"
        kw_args["skiprows"] = 5
    bank_df = pd.read_csv(csv_input, **kw_args)
    bank_df = clean_nationwide_data(bank_df)
    print(bank_df)
    return bank_df


def clean_nationwide_data(df_bank: pd.DataFrame) -> pd.DataFrame:
    """assumes that earliest payments are received the day after the session"""
    next_saturday = session_date.shift(days=8).date()
    df_bank["Date"] = pd.to_datetime(df_bank["Date"])
    df_bank["Account ID"] = df_bank["Account ID"].str[12:]
    df_bank = df_bank.drop(df_bank.loc[df_bank["AC Num"] == "JAMES CLARKE"].index)
    df_bank.loc[df_bank["Account ID"] == "m", "Account ID"] = df_bank["AC Num"].str[:15]
    money_fields = ["Value", "Balance"]
    for mf in money_fields:
        df_bank[mf] = pd.to_numeric(df_bank[mf].str.strip("£"))
    df_bank = df_bank.drop(["AC Num", "Blank"], axis=1)
    df_out = df_bank.loc[(df_bank["Date"] > pd.Timestamp(session_date.date())) &
                         (df_bank["Date"] < pd.Timestamp(next_saturday))]
    print(df_out.info())
    return df_out


def get_latest_nationwide_csv_filename() -> str:
    downloads_folder = pathlib.Path("C:\\Users\\j_a_c\\Downloads")
    file_listing = downloads_folder.glob("Statement Download*.csv")
    if file_listing:
        return str(max(file_listing, key=lambda file: file.stat().st_ctime))
    return ""


def monday_process(pasted_text: str = "") -> None:
    coll = MongoClient().money.badminton
    mongo_date = session_date.datetime
    attendees = coll.find_one({"Date": {"$eq": mongo_date}})["People"]

    per_person_cost = float(input("Please enter the amount charged per person: £"))
    me = "James (Host)"
    if me in attendees:
        record_payment(me, per_person_cost, "host")

    bank_df = create_nationwide_dataset(pasted_text)
    for df_index in bank_df.index:
        account_id = bank_df.loc[df_index]["Account ID"]
        payment_amount = bank_df.loc[df_index]["Value"]
        paying_attendee = find_in_existing_mappings(account_id)
        if not paying_attendee:
            paying_attendee = identify_payer(account_id, payment_amount)
        if paying_attendee:
            record_payment(paying_attendee, payment_amount)
    handle_non_transfer_payments()
    sorting_out_multi_person_payments(per_person_cost)

    after = coll.find_one({"Date": {"$eq": mongo_date}})["People"]
    print(f"Full session details:\n{after}")
    still_unpaid = get_unpaid()
    if still_unpaid:
        print(f"{still_unpaid} have not paid.  That is {len(still_unpaid)} people.")
    payments_string = "\n".join([f"\t£{get_total_payments(after, t):.2f} in {t}"
                                 for t in ("transfer", "host", "cash")])
    print(f"So far have received \n{payments_string}\nfor this session.")


def find_in_existing_mappings(account_id: str) -> str:
    coll = MongoClient().money.badminton
    mappings = coll.find_one({"_id": "AccountMappings"})
    if account_id in mappings:
        alias = mappings[account_id]
        attendees = get_all_attendees()
        if alias in attendees:
            return alias
        if isinstance(alias, list):
            valid_aliases = [*filter(lambda name: name in attendees, alias)]
            if valid_aliases:
                return valid_aliases[0]
    return ""


def identify_payer(account_id: str, amount: float) -> str:
    coll = MongoClient().money.badminton
    mappings = coll.find_one({"_id": "AccountMappings"})
    if account_id in mappings:
        """e.g. Steve L, Ali I: previous alias is not in current session"""
        old_alias = mappings[account_id]
        if isinstance(old_alias, list):
            old_alias = old_alias[0]
        new_alias = get_new_alias_from_input(account_id, amount, clue=old_alias)
        set_new_alias(account_id, new_alias)
    else:
        """previously un-encountered account id"""
        new_alias = get_new_alias_from_input(account_id, amount)
        set_new_alias(account_id, new_alias)
    return new_alias


def handle_non_transfer_payments():
    special_cases = (
        ("cash", "Did anyone else pay in cash?"),
        ("no show", "Were there any more no-shows?")
    )
    for case, question in special_cases:
        if input(f"{question} ") in "yY":
            attendee = True
            while attendee:
                attendee, amount = pick_name_from_unpaid("Who"), 0
                if attendee:
                    if case == "cash":
                        amount = float(input(f"How much did {attendee} pay?\n\t£"))
                    record_payment(attendee, amount, case)
                if not get_unpaid():
                    break
        if not get_unpaid():
            break


def sorting_out_multi_person_payments(per_person_cost: float):
    coll = MongoClient().money.badminton
    for attendee in get_all_attendees():
        session_record = coll.find_one({"Date": {"$eq": session_date.datetime},
                                        "People": {"$exists": True}})
        # TODO: maybe don't need to check existence of "People" key?
        for type_of_payment in ("transfer", "cash", "host"):
            if type_of_payment in session_record["People"][attendee]:
                amount_paid = session_record["People"][attendee][type_of_payment]
                excess = amount_paid - per_person_cost
                while excess > 0.1:
                    options = (
                        f"Pay for someone else",
                        f"Keep all £{amount_paid:.2f} against {attendee}",
                        "Ignore this excess - it is for something else",
                    )
                    choice = input(f"{attendee} has paid an additional "
                                   f"£{excess:.2f}. "
                                   f"What do you want to do with it?\n"
                                   f"{show_options_list(options)}\n")
                    if int(choice) == 1:
                        recipient = pick_name_from_unpaid("Who are they paying for")
                        excess -= per_person_cost
                        amount_paid -= per_person_cost
                        record_payment(attendee, amount_paid, type_of_payment, False)
                        record_payment(recipient, per_person_cost, type_of_payment)
                    elif int(choice) == 2:
                        excess = 0
                    elif int(choice) == 3:
                        excess = 0
                        record_payment(attendee, per_person_cost, type_of_payment, False)


def set_new_alias(account_name: str, alias: object):
    """alias can be string or list of strings"""
    coll = MongoClient().money.badminton
    existing_alias = None
    mappings = coll.find_one({"_id": "AccountMappings"})
    if mappings and account_name in mappings:
        existing_alias = mappings[account_name]
        if isinstance(existing_alias, str):
            existing_alias = [existing_alias]
            alias = existing_alias + [alias]
    coll.update_one({"_id": "AccountMappings"},
                    {"$set": {account_name: alias}})


def pick_name_from_unpaid(question: str) -> str:
    return pick_name_from(get_unpaid(), question)


def pick_name_from(list_of_names: [str], question: str) -> str:
    choice = input(f"{question}?\n{show_options_list(list_of_names)}\n")
    if choice.isnumeric():
        index_chosen = int(choice) - 1
        if index_chosen in range(len(list_of_names)):
            return list_of_names[index_chosen]
    return ""


def get_new_alias_from_input(account_name: str,
                             amount: float, clue: str = "") -> str:
    not_paid = get_unpaid()
    initials = [word[0] for word in clue.title().split()]
    initials += [word[0] for word in account_name.split()]
    right_initials = [*filter(lambda name: name[0] in initials, not_paid)]
    shortlist = sorted(right_initials,
                       key=lambda s: initials.index(s[0]))
    hint = f" (previously known as {clue})" if clue else ""
    for group in (shortlist, not_paid):
        question = f"Who is {account_name}{hint}?  (They paid £{amount:.2f})"
        identified_attendee = pick_name_from(group, question)
        if identified_attendee:
            return identified_attendee
    return ""


def show_options_list(options: [str]) -> str:
    option_list = [f"[{i + 1}] {name}" for i, name in enumerate(options)] + \
                  ["[?] Don't know / Ignore"]
    # TODO: see Vania's train payment.  Want to be able to ignore it the first time
    #       around, not be forced to pick from the list and hit '?' again
    display_string = ""
    max_line_length = 72
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


def record_payment(attendee: str, amount: float,
                   payment_type: str = "transfer",
                   keep_previous_payment: bool = True):
    coll = MongoClient().money.badminton
    previous_amount = 0
    session_record = coll.find_one({"Date": {"$eq": session_date.datetime},
                                    "People": {"$exists": True}})
    people = session_record["People"]
    if keep_previous_payment and session_record and \
            payment_type in session_record["People"][attendee]:
        previous_amount = session_record["People"][attendee][payment_type]
    people[attendee] = {payment_type: previous_amount + amount}
    coll.update_one({"Date": {"$eq": session_date.datetime}},
                    {"$set": {"People": people}})
    print(f"{payment_type} transaction of £{amount:.2f} added for {attendee}")


def get_unpaid() -> [str]:
    coll = MongoClient().money.badminton
    session_people = coll.find_one({"Date": {"$eq": session_date.datetime}})["People"]
    return [k for k in session_people if not session_people[k]]


def get_all_attendees() -> [str]:
    coll = MongoClient().money.badminton
    session_people = coll.find_one({"Date": {"$eq": session_date.datetime}})["People"]
    return [*session_people.keys()]


def get_total_payments(session: dict, payment_type: str = "transfer") -> float:
    return sum([v[payment_type] for v in session.values()
                if isinstance(v, dict) and payment_type in v])


session_date = get_latest_perse_time()


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

