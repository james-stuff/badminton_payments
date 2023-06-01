from pymongo import MongoClient
import arrow
import argparse
import pandas as pd
import pathlib
import google_sheets_interface as gsi
import re


def set_session_date(new_date: arrow.Arrow):
    global session_date
    session_date = new_date


def session_data_from_google_sheet() -> dict:
    return gsi.get_session_data(session_date)


def clean_name_list(names: [str]) -> [str]:
    def extract_name(row: str) -> str:
        if row:
            return row.strip(" @.1234567890").title()
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


def get_current_session() -> dict:
    return coll.find_one({"Date": {"$eq": session_date.datetime}})


def create_session() -> dict:
    # TODO: add an option to delete historic sessions
    google_data = session_data_from_google_sheet()
    mongo_date = session_date.datetime
    new_document = {k: v for k, v in google_data.items() if k != "Col A"}
    new_document["Date"] = mongo_date
    new_document["People"] = {name: {} for name in
                              clean_name_list(google_data["Col A"])}
    coll.insert_one(new_document)
    return new_document


def delete_session():
    coll.delete_many({"Date": {"$eq": session_date.datetime}})


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


def create_nationwide_dataset() -> pd.DataFrame:
    # TODO: fail gracefully if file not found
    #   perhaps also not overwrite a session until valid data is found?
    csv_input = get_latest_nationwide_csv_filename()
    kw_args = {
        "names": ["Date", "Account ID", "AC Num", "Blank", "Value", "Balance"],
        "encoding": "cp1252",
        "skiprows": 5,
    }
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
        return str(max(file_listing, key=lambda file: file.stat().st_mtime))
    return ""


def monday_process() -> None:
    session = get_current_session()
    if not session:
        session = create_session()
    attendees = session["People"]
    rows_to_ignore, rp_key = 0, "Rows Processed"
    if rp_key in session:
        rows_to_ignore = session[rp_key]

    per_person_cost = session["Amount Charged"]
    me = "James (Host)"
    if me in attendees and not rows_to_ignore:
        record_payment(me, per_person_cost, "host")

    bank_df = create_nationwide_dataset()[rows_to_ignore:]
    print(f"=== BANK_DF ===\nOnly looking at:\n{bank_df}")
    for index_num in bank_df.index:
        account_id = bank_df.loc[index_num]["Account ID"]
        payment_amount = bank_df.loc[index_num]["Value"]
        paying_attendee = find_attendee_in_mappings(account_id)
        if not paying_attendee:
            paying_attendee = identify_payer(account_id, payment_amount)
        if paying_attendee:
            if payment_amount >= 2 * per_person_cost:
                payment_amount = pay_obo(paying_attendee, payment_amount,
                                         per_person_cost)
            record_payment(paying_attendee, payment_amount)
    update_rows_processed(len(bank_df))
    if not rows_to_ignore:
        handle_non_transfer_payments()
    sorting_out_excess_payments()

    after = get_current_session()["People"]
    payments_string = "\n".join([f"\t£{get_total_payments(after, t):.2f} in {t}"
                                 for t in ("transfer", "host", "cash")])
    print(f"So far have received \n{payments_string}\nfor this session.")
    still_unpaid = get_unpaid()
    if still_unpaid:
        print(f"{still_unpaid} have not paid.  That is {len(still_unpaid)} people.")


def update_rows_processed(n_rows: int):
    session = get_current_session()
    rows_already_processed = 0
    key = "Rows Processed"
    if key in session:
        rows_already_processed = session[key]
    coll.update_one({"Date": {"$eq": session_date.datetime}},
                    {"$set": {key: rows_already_processed + n_rows}})


def pay_obo(donor: str, transfer_value: float, cost: float) -> float:
    """Automatically allocates cost amount to registered recipients of
    OBO payments from the donor, if they are attendees and while there is
    enough of an excess amount left to cover session cost"""
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
    """for when account name did not match with any attendee name in mappings"""
    mappings = coll.find_one({"_id": "AccountMappings"})
    previous_alias = ""
    if account_id in mappings:
        """e.g. Steve L, Ali I: previous alias is not in current session"""
        previous_alias = mappings[account_id]
        if isinstance(previous_alias, list):
            previous_alias = previous_alias[0]
    """else previously un-encountered account id"""
    new_alias = get_new_alias_from_input(account_id, amount, clue=previous_alias)
    if new_alias.upper() == "H":
        allocate_to_past_session(amount)
        return ""
    elif new_alias.upper() == "I":
        record_incidental_payment("unknown", amount)
        return ""
    elif new_alias:
        set_new_alias(account_id, new_alias)
    return new_alias


def allocate_to_past_session(payment_amount: float,
                             payment_method: str = "transfer"):
    current_session = session_date
    previous_unpaid = {}
    counter = 1
    date_range = {"$gt": arrow.now().shift(days=-90).datetime}
    for session in coll.find({"People": {"$exists": True},
                              "Date": date_range}):
        historic_date = arrow.get(session["Date"])
        set_session_date(historic_date)
        for person in get_unpaid():
            previous_unpaid[counter] = person, historic_date
            counter += 1
    text_options = [f"{p} for {d.format('Do MMM YYYY')}"
                    for p, d in previous_unpaid.values()]
    pu_key = int(input(f"Allocate to whom and when?\n"
                       f"{show_options_list(text_options)}\n"))
    attendee, previous_session = previous_unpaid[pu_key]
    set_session_date(previous_session)
    record_payment(attendee, payment_amount, payment_type=payment_method,
                   keep_previous_payment=True)
    sorting_out_excess_payments()
    set_session_date(current_session)


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


def sorting_out_excess_payments():
    per_person_cost = get_current_session()["Amount Charged"]
    for attendee in get_all_attendees():
        session_record = get_current_session()
        for payment_method in ("transfer", "cash", "host"):
            if payment_method in session_record["People"][attendee]:
                amount_paid = session_record["People"][attendee][payment_method]
                excess = amount_paid - per_person_cost
                while excess > 0.1:
                    allocation_options = (
                        f"Pay for someone else",
                        f"Keep all £{amount_paid:.2f} against {attendee}",
                        "Allocate this excess as incidental payment",
                        "Allocate against another session"
                    )
                    choice = input(f"{attendee} has paid an additional "
                                   f"£{excess:.2f}. "
                                   f"What do you want to do with it?\n"
                                   f"{show_options_list(allocation_options)}\n")
                    if choice.isnumeric():
                        if int(choice) == 1:
                            recipient = pick_name_from_unpaid("Who are they paying for")
                            excess -= per_person_cost
                            amount_paid -= per_person_cost
                            record_payment(attendee, amount_paid, payment_method, False)
                            record_payment(recipient, per_person_cost, payment_method)
                            add_to_payments_obo(attendee, recipient)
                        elif int(choice) == 2:
                            excess = 0
                        elif int(choice) == 3:
                            record_incidental_payment(attendee, excess)
                            excess = 0
                            record_payment(attendee, per_person_cost, payment_method, False)
                        elif int(choice) == 4:
                            allocate_to_past_session(excess, payment_method)
                            excess = 0
                            record_payment(attendee, per_person_cost, payment_method, False)


def record_incidental_payment(attendee: str, amount: float):
    purpose = input("What was this payment for?\n")
    query = {"_id": "IncidentalPayments"}
    record = coll.find_one(query)
    date_string = session_date.format("YYYYMMDD")
    if not record:
        record = query
        coll.insert_one(query)
    if date_string not in record:
        record[date_string] = {}
    record[date_string][attendee] = {"amount": amount, "purpose": purpose}
    coll.update_one(query, {"$set": record})


def add_to_payments_obo(donor: str, recipient: str):
    query = {"_id": "PaymentsOBO"}
    record = coll.find_one(query)
    if (donor in record) and (recipient not in record[donor]):
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


def pick_name_from(list_of_names: [str], question: str,
                   names_plus_options: bool = False) -> str:
    other_options = {
        "H": "Allocate against another session",
        "I": "Record as incidental payment",
        "?": "Don't know",
    } if names_plus_options else {}
    choice = input(f"{question}?\n{show_options_list(list_of_names, other_options)}\n")
    if choice.isnumeric():
        index_chosen = int(choice) - 1
        if index_chosen in range(len(list_of_names)):
            return list_of_names[index_chosen]
    if choice.upper() == "H":
        return choice
    if choice.upper() == "I":
        # TODO: how to pass the name of the payer?
        return choice
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
        identified_attendee = pick_name_from(group, question, names_plus_options=True)
        if identified_attendee:
            return identified_attendee
    return ""


def show_options_list(numbered_choices: [str], breakout_options: dict = {}) -> str:
    remaining_options = [f"[{i + 1}] {c}" for i, c in enumerate(numbered_choices)] + \
                        [f"[{k}] {v}" for k, v in breakout_options.items()]
    display_string = ""
    max_line_length = 72
    # algorithm courtesy of ChatGPT:
    current_line_length = 0
    for option_text in remaining_options:
        if current_line_length + len(option_text) > max_line_length:
            display_string += "\n"
            current_line_length = 0
        display_string += f"\t{option_text}"
        current_line_length += len(option_text) + 1
    return display_string


def record_payment(attendee: str, amount: float,
                   payment_type: str = "transfer",
                   keep_previous_payment: bool = True):
    previous_amount = 0
    session_record = get_current_session()
    people = session_record["People"]
    if keep_previous_payment and session_record and \
            payment_type in session_record["People"][attendee]:
        previous_amount = session_record["People"][attendee][payment_type]
    people[attendee] = {payment_type: previous_amount + amount}
    coll.update_one({"Date": {"$eq": session_date.datetime}},
                    {"$set": {"People": people}})
    print(f"{payment_type} transaction of £{amount:.2f} added for {attendee}")


def get_unpaid() -> [str]:
    session_people = get_current_session()["People"]
    return [*filter(lambda k: not session_people[k], session_people.keys())]


def get_all_attendees() -> [str]:
    session_people = get_current_session()["People"]
    return [*session_people.keys()]


def get_total_payments(session_people: dict, payment_type: str = "transfer") -> float:
    return sum([v[payment_type] for v in session_people.values()
                if isinstance(v, dict) and payment_type in v])


def generate_sign_up_message(wa_pasting: str, host: str = "James") -> str:
    friday = time_machine(arrow.now().shift(days=7))
    header = f"Perse Upper School, " \
             f"{friday.format('dddd, Do MMMM YYYY')}, 19:30 - 21:30:" \
             f"\n\nUp to 6 courts, max. 33 players\n\n"

    names = [f"{host} (Host)"]

    time_regex = r"[[0-2][0-9]:[0-5][0-9], [0-3][0-9]/[0-1][0-9]/20[0-9][0-9]] "
    ends = [i.end() for i in re.finditer(time_regex, wa_pasting)]
    if ends:
        for ind, e in enumerate(ends):
            message = wa_pasting[e:e + 10000 if ind == len(ends) - 1 else ends[ind + 1] - 21]
            lines = []
            for line in message.split('\n'):
                sender, _, body = line.partition(": ")
                lines.append(body if body else sender)
            if len(lines[0]) < 21:
                names += [ln for ln in lines if ln]
    else:
        names += [ln for ln in wa_pasting.split("\n")
                  if 0 < len(ln) < 21 or "friend)" in ln]

    while len(names) < 35:
        names.append("")
    people_with_a_spot = [f"{i + 1}. {nm}"
                          for i, nm in enumerate(names[:33])]
    in_list = "\n".join(people_with_a_spot)
    waitlist = "\n".join([f"{chr(97 + j)}. {wnm}" for j, wnm in enumerate(names[33:])])
    return f"{header}{in_list}\n\nWAITLIST:\n{waitlist}\n...\n" \
           f"(copy and paste, adding your name to secure a spot)"


def create_next_session_sheet():
    """add a new sheet to the Google sheet for the month in required format"""
    next_friday = time_machine(get_latest_perse_time().shift(days=7))
    print(f"This'll create a sheet for {next_friday.format('Do MMM')}")
    gsi.create_new_session_sheet(next_friday)
    # TODO: can I make the new sheet the one you land on when opening spreadsheet?
    #   From google: "How long do Google API tokens last?
    # A Google Cloud Platform project with an OAuth consent screen configured
    #  for an external user type and a publishing status of "Testing" is
    #  issued a refresh token expiring in 7 days. There is currently a
    #  limit of 100 refresh tokens per Google Account per OAuth 2.0 client ID.
    #  23/01/2023 - the 100 token limit seems is just per client ID.  Solution
    #  is probably to simply create a new client ID if and when that happens
    #  (some time around September 2024 if it's still going by then)"
    # TODO: running 'Friday' process on a Monday generates a tab for the
    #       upcoming Friday, not the previous one


def court_rate_in_force(date: arrow.Arrow) -> float:
    rates = coll.find_one({"_id": "PerseRates"})
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
                  "$lt": first_of_month.ceil("month").datetime}}).sort("Date")
    print(f"\nExpected Perse School Invoice for "
          f"{first_of_month.format('MMMM YYYY').upper()}:")
    print(f"\nDate\tCourts\tCost\tTransfers")
    total_cost, total_transfers = 0, 0
    for s in sessions:
        date = arrow.get(s['Date'])
        print(f"{date.format('Do'):>7}\t", end="")
        if "Venue" in s.keys():     # This key is added to DB manually, for now
            venue = s['Venue']
            print(f"({venue} session)")
        else:
            cost = int(s['Courts']) * 2 * court_rate_in_force(date)
            transfers = get_total_payments(s['People'])
            print(f"{s['Courts']:>6}\t£{cost:>6.2f}\t£{transfers:>6.2f}")
            total_cost += cost
            total_transfers += transfers
    print("")
    print(f"Totals:\t\t£{total_cost:>6.2f}\t£{total_transfers:>6.2f}")
    incidentals = coll.find_one({"_id": "IncidentalPayments"})
    recs = [v for k, v in incidentals.items() if k[:6] == f"{date.format('YYYYMM')}"]
    inc_total = sum([v for r in recs for person in r.values() for k, v in person.items() if k == 'amount'])
    print(f"Incidental transfers:\t£{inc_total:>6.2f}")
    print(f"Total to move:\t\t£{total_transfers + inc_total:>6.2f}")


def show_session_details(session: {}):
    print(f"Session details for {session['Date'].date()}")


def details_for_past_n_sessions(n: int = 5) -> {}:
    query = coll.find({"Date": {"$exists": True}}).sort("Date", 1)
    sessions = [*query][-n:]
    details = {}
    for sess in sessions:
        arrow_date = arrow.get(sess["Date"])
        set_session_date(arrow_date)
        unpaid = ", ".join(get_unpaid())
        details[arrow_date] = f'{arrow_date.format("Do MMM YYYY"):>13}' \
                              f'{sess["In Attendance"]:>8}  ' \
                              f'£{sess["Amount Charged"]:>4.2f} ' \
                              f'{unpaid:<32}'
    return details


def show_past_n_sessions(no_of_sessions: int = 5):
    print("\nRecent sessions:")
    print(f"{'Date':>13}  People   Cost Unpaid")
    print("\n".join(details_for_past_n_sessions(no_of_sessions).values()))


def allow_reprocessing_of_previous_n_sessions(n: int = 5):
    print("\nPick a session to re-process:")
    details = details_for_past_n_sessions(n)
    display_rows = [*details.values()]
    display_rows = [f" {d}" for d in display_rows[:9]] + display_rows[9:]
    input_mapping = {i + 1: dt for i, dt in enumerate(details.keys())}
    print(f"\t\t     {'Date':>13}  People   Cost Unpaid")
    print(show_options_list(display_rows))
    picked = int(input(''))
    if picked in input_mapping:
        set_session_date(input_mapping[picked])
        monday_process()


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


def show_paid_invoices():
    current_ac = MongoClient().money.current_account
    start_day = arrow.now().shift(days=-180).floor("month")
    print("\nRecently paid Perse School invoices:")
    for rec in current_ac.find({"Date": {"$gte": start_day.datetime},
                                "Party": {"$regex": " SP[0-9]{3} "}}):
        print(f"\t{arrow.get(rec['Date']).format('DD MMM YYYY')}\t"
              f"{rec['Party'][62:67]}\t£{-rec['Value']:,.2f}")


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
        "P": show_paid_invoices,
        "O": show_past_n_sessions,
        "R": allow_reprocessing_of_previous_n_sessions,
    }
    if op in options:
        options[op]()
    else:
        print(f"{op} is not a valid operation code")
