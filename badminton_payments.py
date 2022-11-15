from pymongo import MongoClient
import arrow
import argparse
import pandas as pd
from io import StringIO
import pathlib
import google_sheets_interface as gsi


def set_session_date(new_date: arrow.Arrow):
    global session_date
    session_date = new_date


def session_data_from_google_sheet() -> dict:
    return gsi.get_session_data(session_date)


def list_of_names_from_whatsapp(pasted_list: str) -> [str]:
    """used only in testing"""
    return clean_name_list(pasted_list.split("\n"))


def clean_name_list(names: [str]) -> [str]:
    def extract_name(row: str) -> str:
        if row:
            return row.strip(" .1234567890").title()
        return ""

    names = [*filter(lambda x: x, [extract_name(n) for n in names])]
    names = ensure_uniqueness(names)
    return names


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


def create_session():
    google_data = session_data_from_google_sheet()
    mongo_date = session_date.datetime
    datetime_query = {"Date": {"$eq": mongo_date}}
    if coll.find_one(datetime_query):
        if input(f"Session already exists for {session_date.format('ddd Do MMMM')}, "
                 f"overwrite? ") in "nN":
            # TODO: this alone does not stop the session being processed
            #       and will result in double-counted payments
            return
        coll.delete_many(datetime_query)
        # TODO: maybe allow editing here?  Or select a previous session
    document = {k: v for k, v in google_data.items() if k != "Col A"}
    document["Date"] = mongo_date
    document["People"] = {name: {} for name in clean_name_list(google_data["Col A"])}
    coll.insert_one(document)


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


def create_nationwide_dataset(passed_data: str = "") -> pd.DataFrame:
    # TODO: fail gracefully if file not found
    #   perhaps also not overwrite a session until valid data is found?
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
    """assumes Sat. post-session to Fri. inclusive window in which payments are made"""
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
    file_listing = [*downloads_folder.glob("Statement Download*.csv")]
    if file_listing:
        return str(max(file_listing, key=lambda file: file.stat().st_ctime))
    return ""


def monday_process() -> None:
    create_session()
    mongo_date = session_date.datetime
    session = coll.find_one({"Date": {"$eq": mongo_date}})
    attendees = session["People"]

    # per_person_cost = float(input("Please enter the amount charged per person: £"))
    per_person_cost = session["Amount Charged"]
    me = "James (Host)"
    if me in attendees:
        record_payment(me, per_person_cost, "host")

    bank_df = create_nationwide_dataset()
    for df_index in bank_df.index:
        account_id = bank_df.loc[df_index]["Account ID"]
        payment_amount = bank_df.loc[df_index]["Value"]
        paying_attendee = find_attendee_in_mappings(account_id)
        if not paying_attendee:
            paying_attendee = identify_payer(account_id, payment_amount)
        if paying_attendee:
            if payment_amount >= 2 * per_person_cost:
                payment_amount = pay_obo(paying_attendee, payment_amount,
                                         per_person_cost)
            record_payment(paying_attendee, payment_amount)
    handle_non_transfer_payments()
    sorting_out_multi_person_payments(per_person_cost)

    after = coll.find_one({"Date": {"$eq": mongo_date}})["People"]
    payments_string = "\n".join([f"\t£{get_total_payments(after, t):.2f} in {t}"
                                 for t in ("transfer", "host", "cash")])
    print(f"So far have received \n{payments_string}\nfor this session.")
    still_unpaid = get_unpaid()
    if still_unpaid:
        print(f"{still_unpaid} have not paid.  That is {len(still_unpaid)} people.")


def pay_obo(donor: str, transfer_value: float, cost: float) -> float:
    # TODO: write paying account id?
    doc_obo = coll.find_one({"_id": "PaymentsOBO"})
    if donor not in doc_obo:
        return transfer_value
    amount_remaining = transfer_value
    possibles = filter(lambda a: a in doc_obo[donor], get_unpaid())
    for p in possibles:
        if amount_remaining > cost:
            record_payment(p, cost)
            amount_remaining -= cost
    return amount_remaining


def find_attendee_in_mappings(account_id: str) -> str:
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
    """for when account name did not match with any attendee name"""
    mappings = coll.find_one({"_id": "AccountMappings"})
    old_alias = ""
    if account_id in mappings:
        """e.g. Steve L, Ali I: previous alias is not in current session"""
        old_alias = mappings[account_id]
        if isinstance(old_alias, list):
            old_alias = old_alias[0]
    """else previously un-encountered account id"""
    new_alias = get_new_alias_from_input(account_id, amount, clue=old_alias)
    if new_alias:
        set_new_alias(account_id, new_alias)
    return new_alias


def handle_non_transfer_payments():
    special_cases = (
        ("cash", "How many people paid in cash?"),
        ("no show", "How many no-shows were there?")
    )
    for case, question in special_cases:
        if not get_unpaid():
            break
        no_of_people = int(input(f"{question} "))
        for _ in range(no_of_people):
            attendee, amount = pick_name_from_unpaid("Who"), 0
            if attendee:
                if case == "cash":
                    amount = float(input(f"How much did {attendee} pay?\n\t£"))
                record_payment(attendee, amount, case)


def sorting_out_multi_person_payments(per_person_cost: float):
    for attendee in get_all_attendees():
        session_record = coll.find_one({"Date": {"$eq": session_date.datetime}})
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
                        add_to_payments_obo(attendee, recipient)
                    elif int(choice) == 2:
                        excess = 0
                    elif int(choice) == 3:
                        excess = 0
                        record_payment(attendee, per_person_cost, type_of_payment, False)


def add_to_payments_obo(donor: str, recipient: str):
    query = {"_id": "PaymentsOBO"}
    record = coll.find_one(query)
    if donor in record:
        record[donor] = record[donor] + [recipient]
    else:
        record[donor] = [recipient]
    coll.update_one(query, {"$set": record})


def set_new_alias(account_name: str, alias: str):
    """alias can be string or list of strings"""
    mappings = coll.find_one({"_id": "AccountMappings"})
    if mappings and account_name in mappings:
        existing_alias = mappings[account_name]
        list_to_add_to = existing_alias if isinstance(existing_alias, list) \
            else [existing_alias]
        list_to_add_to.append(alias)
        alias = list_to_add_to
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
        # TODO: should be able to choose to Ignore first time around
        #   (e.g. for someone who paid for something else and therefore
        #     is not on the attendee list)
        # TODO: also would like to be able to allocate against past session
        question = f"Who is {account_name}{hint}?  (They paid £{amount:.2f})"
        identified_attendee = pick_name_from(group, question)
        if identified_attendee:
            return identified_attendee
    return ""


def show_options_list(options: [str]) -> str:
    option_list = [f"[{i + 1}] {name}" for i, name in enumerate(options)] + \
                  ["[?] Don't know", "[I] Ignore"]
    # TODO: see Vania's train payment.  Want to be able to ignore it the first time
    #       around, not be forced to pick from the list and hit '?' again
    # TODO: consider adding the ability to allocate payment to a previous session
    # ToDO: probably add one-off payments to a separate document so can view
    #       at end of month
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
    session_people = coll.find_one({"Date": {"$eq": session_date.datetime}})["People"]
    return [k for k in session_people if not session_people[k]]


def get_all_attendees() -> [str]:
    session_people = coll.find_one({"Date": {"$eq": session_date.datetime}})["People"]
    return [*session_people.keys()]


def get_total_payments(session_people: dict, payment_type: str = "transfer") -> float:
    return sum([v[payment_type] for v in session_people.values()
                if isinstance(v, dict) and payment_type in v])


def generate_sign_up_message(wa_pasting: str, host: str = "James") -> str:
    friday = time_machine(arrow.now().shift(days=7))
    header = f"Booked (by James), Perse Upper School, " \
             f"{friday.format('dddd, Do MMMM YYYY')}, 19:30 - 21:30:" \
             f"\n\nUp to 6 courts, max. 33 players\n\n"

    def extract_name(raw_data: str) -> str:
        subsequent_name, _, name = raw_data.partition(': ')
        if subsequent_name and "[" not in subsequent_name:
            return subsequent_name
        return name

    names = [f"{host} (Host)"] + [extract_name(m) for m in wa_pasting.split('\n')]
    names = [*filter(lambda text: text and len(text.split(" ")) < 3, names)]
    in_list = "\n".join([f"{i + 1}. {nm}" for i, nm in enumerate(names[:33])])
    waitlist = "\n".join([f"{chr(97 + j)}. {wnm}" for j, wnm in enumerate(names[33:])])
    return f"{header}{in_list}\n\nWAITLIST:\n{waitlist}\n...\n" \
           f"(copy and paste, adding your name to secure a spot)"


def create_next_session_sheet():
    """add a new sheet to the Google sheet for the month in required format"""
    next_friday = time_machine(get_latest_perse_time().shift(days=7))
    print(f"This'll create a sheet for {next_friday.format('Do MMM')}")
    gsi.create_new_session_sheet(next_friday)
    # TODO: should set courts to 6 and cash payments to zero
    #  and can I make the new sheet the one you land on when opening spreadsheet?
    # TODO: how long do the credentials stay valid for?
    #       Maybe delete the token file if it is of more than a certain age?
    #   From google: "How long do Google API tokens last?
    # A Google Cloud Platform project with an OAuth consent screen configured
    #  for an external user type and a publishing status of "Testing" is
    #  issued a refresh token expiring in 7 days. There is currently a
    #  limit of 100 refresh tokens per Google Account per OAuth 2.0 client ID."
    # TODO: process of creating a new tab didn't work when new monthly sheet
    #       needed to be created (for 4th Nov session)
    # TODO: running 'Friday' process on a Monday generates a tab for the
    #       upcoming Friday, not the previous one


def court_rate_in_force(date: arrow.Arrow) -> float:
    rates = coll.find_one({"_id": "Perse Rates"})
    del rates["_id"]
    latest_date = max([k for k in rates.keys() if arrow.get(k) <= date])
    return rates[latest_date]


def invoices():
    req_month = input("Which month would you like to look at? [MM(-YY)] ")
    year = arrow.now().year
    if len(req_month) < 3:
        month = int(req_month)
    else:
        month, _, yy = req_month.partition("-")
        month, year = int(month), int(f"20{yy}")
    first_of_month = arrow.Arrow(year, month, 1)
    sessions = coll.find(
        {"Date": {"$gt": first_of_month.datetime,
        # {"Date": {"$gt": arrow.Arrow(year, month, 14).datetime,
                  "$lt": first_of_month.ceil("month").datetime}})
    print(f"\nExpected Perse School Invoice for "
          f"{first_of_month.format('MMMM YYYY').upper()}:")
    print(f"\nDate\tCourts\tCost\tTransfers")
    for s in sessions:
        date = arrow.get(s['Date'])
        cost = int(s['Courts']) * 2 * court_rate_in_force(date)
        print(f"{date.format('Do'):>7}\t{s['Courts']:>6}\t£{cost:>6.2f}"
              f"\t£{get_total_payments(s['People']):>6.2f}")
        # TODO: show total for month


def historic_session():
    text_date = input(f"Process a historic session.  Enter date as DDMM "
                      f"(plus YY if looking at a previous year):\n\t")
    y = arrow.now().year
    date_elements = (int(text_date[i:i + 2]) for i in range(0, len(text_date), 2))
    if len(text_date) <= 4:
        d, m = date_elements
    else:
        d, m, y = date_elements
        y += 2000
    set_session_date(time_machine(arrow.Arrow(y, m, d)))
    monday_process()


session_date = get_latest_perse_time()
coll = MongoClient().money.badminton


if __name__ == "__main__":
    my_parser = argparse.ArgumentParser(description='Badminton payments processing')
    my_parser.add_argument('Operation',
                           metavar='operation',
                           type=str,
                           help='either [F] set up a new session or [M] process payments for existing session')
    args = my_parser.parse_args()
    op = args.Operation.upper()

    options = {
        "M": monday_process,
        "F": create_next_session_sheet,
        "I": invoices,
        "H": historic_session,
    }
    if op in options:
        options[op]()
    else:
        print(f"{op} is not a valid operation code")

