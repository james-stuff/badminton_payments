from pymongo import MongoClient
import arrow
import argparse
import pandas as pd
from io import StringIO
import pathlib


def set_session_date(new_date: arrow.Arrow):
    global session_date
    session_date = new_date


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


def create_session(people: [str]):
    collection = MongoClient().money.badminton
    mongo_date = session_date.datetime
    date_query = {"Date": {"$eq": mongo_date}}
    if collection.find_one(date_query):
        if input(f"Session already exists for {session_date.format('ddd Do MMMM')}, "
                 f"overwrite? ") in "nN":
            return
        collection.delete_many(date_query)
    document = {name: {} for name in people}
    document["Date"] = mongo_date
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


def time_machine(requested_date: arrow.Arrow) -> arrow.Arrow:
    weeks_back = ((arrow.now() - requested_date).days - 1) // 7
    return latest_perse_time().shift(days=-7 * weeks_back)


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
    # TODO: second Mohan Krishna payment for Kelsey Kerridge makes things interesting!
    #       actually gets ignored because he doens't show up in unpaid
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
    session = coll.find_one({"Date": {"$eq": mongo_date}})

    # per_person_cost = float(input("Please enter the amount charged per person: £"))
    per_person_cost = 4.4
    me = "James (Host)"
    if me in session:
        record_payment(me, per_person_cost, "host")

    bank_df = create_nationwide_dataset(pasted_text)
    # TODO: what if there are two payments from the same account?
    for df_index in bank_df.index:
        account_id = bank_df.loc[df_index]["Account ID"]
        payment_amount = bank_df.loc[df_index]["Value"]
        paying_attendee = find_in_existing_mappings(account_id)
        if not paying_attendee:
            paying_attendee = identify_payer(account_id, payment_amount)
        if paying_attendee:
            record_payment(paying_attendee, payment_amount)

    while input("Did anyone pay in cash? ") in "yY":
        # TODO: this (and the no-shows loop) show everyone in the list of options
        # TODO: break out of loop if there are no more unpaid
        unpaid = get_unpaid()
        choice = int(input(f"Who?\n{choice_of_names(unpaid)}\n"))
        cash_amount = float(input(f"How much did they pay? £"))
        # print(f"You are telling me {unpaid[choice]} has paid £{amount}")
        record_payment(unpaid[choice - 1], cash_amount, "cash")

    while input("Were there any no-shows? ") in "yY":
        unpaid = get_unpaid()
        choice = int(input(f"Who?\n{choice_of_names(unpaid)}\n"))
        record_payment(unpaid[choice - 1], 0, "no show")

    sorting_out_multi_person_payments(per_person_cost)

    after = coll.find_one({"Date": {"$eq": mongo_date}})
    print(f"Found session:\n{after}")
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
    """Steve L, Ali I: previous alias is not in current session"""
    # TODO: test that this works the second time round (i.e. once in mappings)
    coll = MongoClient().money.badminton
    mappings = coll.find_one({"_id": "AccountMappings"})
    if account_id in mappings:
        old_alias = mappings[account_id]
        if isinstance(old_alias, list):
            old_alias = old_alias[0]
        new_alias = get_new_alias_from_input(f"{old_alias} / {account_id}", amount)
        set_new_alias(account_id, new_alias)
    else:
        """previously un-encountered account id"""
        new_alias = get_new_alias_from_input(account_id, amount)
        set_new_alias(account_id, new_alias)
    return new_alias


def sorting_out_multi_person_payments(per_person_cost: float):
    coll = MongoClient().money.badminton
    for attendee in get_all_attendees():
        payment_record = coll.find_one({"Date": {"$eq": session_date.datetime},
                                        attendee: {"$exists": True}})
        for type_of_payment in ("transfer", "cash", "host"):
            if type_of_payment in payment_record[attendee]:
                # print(f"{attendee} paid {}")
                amount_paid = payment_record[attendee][type_of_payment]
                if amount_paid > per_person_cost:
                    excess = amount_paid - per_person_cost
                    allocate_option = f"Allocate £{excess:.2f} to someone else"
                    use_all_option = f"Keep all £{amount_paid:.2f} against {attendee}"
                    options = [allocate_option, use_all_option, "Ignore this payment"]
                    option = input(f"{attendee} has paid an additional "
                                   f"£{excess:.2f}. "
                                   f"What do you want to do with it?\n"
                                   f"{choice_of_names(options)}\n")
                    if int(option) == 1:
                        recipient = get_new_alias_from_input("", 0)
                        record_payment(attendee, per_person_cost, type_of_payment, False)
                        record_payment(recipient, excess, type_of_payment)
                    elif int(option) == 3:
                        record_payment(attendee, per_person_cost, type_of_payment, False)


            # TODO: Don't think this has happened yet, but what if there
            #       are multiple payments from the same account for the same
            #       session?  (e.g. person pays for themselves, then remembers
            #       they also need to pay for their other half)
            #       Maybe should just create the session afresh on each run?


def get_new_alias_from_input(account_name: str, amount: float) -> str:
    not_paid = get_unpaid()
    initials = [word[0] for word in account_name.split()]
    right_initials = [*filter(lambda name: name[0] in initials, not_paid)]
    shortlist = sorted(right_initials,
                       key=lambda s: initials.index(s[0]))
    for group in (shortlist, not_paid):
        keyed = input(f"Who is {account_name}?  "
                      f"(They paid £{amount:.2f})\n{choice_of_names(group)}\n")
        if keyed.isnumeric() and int(keyed) in range(1, len(group) + 1):
            new_alias = group[int(keyed) - 1]
            if account_name:
                set_new_alias(account_name, new_alias)
            return new_alias
    return ""


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


def choice_of_names(names: [str]) -> str:
    option_list = [f"[{i + 1}] {name}" for i, name in enumerate(names)] + \
                  ["[?] Don't know / Ignore"]
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
    attendee_record = coll.find_one({"Date": {"$eq": session_date.datetime},
                                     attendee: {"$exists": True}})
    if keep_previous_payment and attendee_record and \
            payment_type in attendee_record[attendee]:
        previous_amount = attendee_record[attendee][payment_type]
    coll.update_one({"Date": {"$eq": session_date.datetime}},
                    {"$set": {attendee: {payment_type: previous_amount + amount}}})
    print(f"{payment_type} transaction of £{amount} added for {attendee}")


def get_unpaid() -> [str]:
    coll = MongoClient().money.badminton
    session = coll.find_one({"Date": {"$eq": session_date.datetime}})
    return [k for k in session if k not in ["Date", "_id"] and not session[k]]


def get_all_attendees() -> [str]:
    coll = MongoClient().money.badminton
    session = coll.find_one({"Date": {"$eq": session_date.datetime}})
    return [k for k in session if k not in ["Date", "_id"]]


def get_total_payments(session: dict, payment_type: str = "transfer") -> float:
    return sum([v[payment_type] for v in session.values()
                if isinstance(v, dict) and payment_type in v])


session_date = latest_perse_time()


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

