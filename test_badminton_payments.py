import badminton_payments as bad_pay
import bp_test_inputs
import arrow
from pymongo import MongoClient
import shutil
import os
import google_sheets_interface as gsi


coll = MongoClient().money.badminton


def copy_test_file_to_downloads(filename: str):
    root = "C:\\Users\\j_a_c"
    source = f"{root}\\Python Stuff\\Money\\Test Files\\{filename}"
    destination = f"{root}\\Downloads\\{filename}"
    shutil.copy(source, destination)


def test_latest_perse_session_time():
    last_friday_at_19_30 = bad_pay.get_latest_perse_time()
    assert last_friday_at_19_30.format("dddd") == "Friday"
    assert (arrow.now() - last_friday_at_19_30).days < 7
    assert last_friday_at_19_30.time().hour == 19
    assert last_friday_at_19_30.time().minute == 30


def test_time_machine_dates():
    assert bad_pay.time_machine(arrow.Arrow(2022, 8, 5)) == arrow.Arrow(2022, 8, 5, 19, 30, tzinfo="local")
    assert bad_pay.time_machine(arrow.Arrow(2022, 6, 6)) == arrow.Arrow(2022, 6, 3, 19, 30, tzinfo="local")
    assert bad_pay.time_machine(arrow.Arrow(2022, 2, 15)) == arrow.Arrow(2022, 2, 11, 19, 30, tzinfo="local")
    assert bad_pay.time_machine(arrow.Arrow(2022, 11, 1)) == arrow.Arrow(2022, 10, 28, 19, 30, tzinfo="local")
    assert bad_pay.time_machine(arrow.Arrow(2022, 10, 6)) == arrow.Arrow(2022, 9, 30, 19, 30, tzinfo="local")
    assert bad_pay.time_machine(arrow.Arrow(2022, 10, 7)) == arrow.Arrow(2022, 10, 7, 19, 30, tzinfo="local")
    assert bad_pay.time_machine(arrow.Arrow(2022, 10, 8)) == arrow.Arrow(2022, 10, 7, 19, 30, tzinfo="local")


def test_name_list_generator():
    names = bad_pay.clean_name_list(bp_test_inputs.aug_5th_list.split('\n'))
    assert len(names) == 31
    names_found = bad_pay.clean_name_list(bp_test_inputs.test_sample_of_names.split('\n'))
    assert len(names_found) == 34
    expected_names = ["Steve L", "James (Host)", "He-Ling", "Kevin K", "André"]
    for n in expected_names:
        assert n in names_found
    assert names_found.count("Kevin K") == 1


def clean_downloads_folder():
    dl_folder = "C:\\Users\\j_a_c\\Downloads"
    for filename in os.listdir(dl_folder):
        if filename.startswith("Statement Download "):
            os.remove(f"{dl_folder}\\{filename}")


def test_two_stage_process_recording_no_of_rows_processed():
    copy_test_file_to_downloads("Statement Download 2022-Aug-15 interim.csv")
    test_date = bad_pay.time_machine(arrow.Arrow(2022, 8, 12))
    bad_pay.set_session_date(test_date)
    bad_pay.delete_session()
    bad_pay.monday_process()
    new_state = bad_pay.get_current_session()
    rp_key = "Rows Processed"
    assert rp_key in new_state
    assert new_state[rp_key] == 19
    copy_test_file_to_downloads("Statement Download 2022-Aug-18 complete.csv")
    bad_pay.monday_process()
    final_state = bad_pay.get_current_session()
    assert rp_key in final_state
    assert final_state[rp_key] == 28
    bad_pay.monday_process()


def test_aug_5th():
    # TODO: incidental payment option is sometimes "I", sometimes a number
    print("\n5th Aug.  Session cost: £4.40.  Josy paid £4.50 in cash.\n"
          "Prameen and Moz no shows.  V paid £12 for train ticket")
    copy_test_file_to_downloads("Statement Download 2022-Aug-15 interim.csv")
    test_date = bad_pay.time_machine(arrow.Arrow(2022, 8, 5))
    bad_pay.set_session_date(test_date)
    bad_pay.delete_session()
    bad_pay.monday_process()
    end_state = coll.find_one({"Date": {"$eq": test_date.datetime}})
    date_of_session = end_state["Date"]
    assert date_of_session.day, date_of_session.hour == (5, 19)
    assert len(end_state) == 7
    assert len(end_state["People"]) == 31
    payment_totals = {"transfer": 114.64, "cash": 4.5, "no show": 0.0, "host": 4.4}
    assert end_state["People"]["James (Host)"]["host"] == 4.4
    assert end_state["People"]["Divik"]["transfer"] == 4.4
    assert end_state["People"]["Karlo"]["transfer"] == 4.4
    assert end_state["People"]["Alex"]["transfer"] == 4.4
    assert all("no show" in end_state["People"][k] for k in ("Moz", "Prameen"))
    for k, v in payment_totals.items():
        assert round(bad_pay.get_total_payments(end_state["People"], k), 2) == v
    clean_downloads_folder()


def test_aug_12th():
    print("\n12th Aug.  Session cost: £4.40.  Josy paid exact amount in cash.\n"
          "Mohan paid an additional £5.00 for Kelsey Kerridge session")
    copy_test_file_to_downloads("Statement Download 2022-Aug-18 complete.csv")
    test_date = arrow.Arrow(2022, 8, 12)
    bad_pay.set_session_date(bad_pay.time_machine(test_date))
    bad_pay.delete_session()
    bad_pay.monday_process()
    end_state = coll.find_one({"Date": {"$eq": bad_pay.time_machine(test_date).datetime}})
    attendees = end_state["People"]
    date_of_session = end_state["Date"]
    assert date_of_session.day, date_of_session.hour == (12, 19)
    assert len(end_state["People"]) == 29
    assert end_state["People"]["James (Host)"]["host"] == 4.4
    assert end_state["People"]["Mohan"]["transfer"] == 4.4    # paid separately for Kelsey Kerridge session a few days later
    assert end_state["People"]["Karlo"]["transfer"] == 4.4
    assert all("no show" not in end_state["People"][k] for k in attendees)
    payment_totals = {"transfer": 118.80, "cash": 4.4, "no show": 0.0, "host": 4.4}
    for k, v in payment_totals.items():
        assert round(bad_pay.get_total_payments(end_state["People"], k), 2) == v
    clean_downloads_folder()


def test_aug_19th():
    print("\n19th Aug.  Session cost: £4.94.  Josy paid £5.00 in cash.\n"
          "Karlo paid for both Alexes, Kevin K no-show, Moz didn't pay,\n"
          "V paid me for a train ticket but no badminton")
    friday = arrow.Arrow(2022, 8, 19)
    bad_pay.set_session_date(bad_pay.time_machine(friday))
    bad_pay.delete_session()
    copy_test_file_to_downloads("Statement Download 2022-Aug-22 14-46-37 interim.csv")
    bad_pay.monday_process()
    karlos_gang = ("Karlo", "Alex", "Alex H")
    record = coll.find_one({"Date": {"$eq": bad_pay.session_date.datetime}})["People"]
    for person in karlos_gang:
        print(f"Data for {person}: {record[person]}")
        assert round(record[person]["transfer"], 2) == 4.94
    assert record["Josy"]["cash"] == 5
    assert record["Kevin K"]["no show"] == 0
    copy_test_file_to_downloads("Statement Download 2022-Oct-20 19-46-16-THREE MONTHS.csv")
    bad_pay.monday_process()
    record = coll.find_one({"Date": {"$eq": bad_pay.session_date.datetime}})["People"]
    assert record["Josy"]["cash"] == 5
    assert record["Kevin K"]["no show"] == 0
    assert record["Mara"]["transfer"] == 4.94
    clean_downloads_folder()


def test_updating_existing_payments():
    bad_pay.set_session_date(bad_pay.time_machine(arrow.Arrow(2022, 8, 5)))
    person = "Josy"
    record = coll.find_one({"Date": {"$eq": bad_pay.session_date.datetime}})
    print(record["People"][person])
    bad_pay.record_payment(person, 1, "cash")
    record = coll.find_one({"Date": {"$eq": bad_pay.session_date.datetime}})
    print(record["People"][person])
    assert record["People"][person]["cash"] == 5.50


def test_reading_from_google_sheets():
    bad_pay.set_session_date(bad_pay.time_machine(arrow.Arrow(2023, 1, 13)))
    sheet_id = gsi.get_spreadsheet_id(arrow.Arrow(2022, 10, 20))
    assert sheet_id == "1c3iSSQNEa8A7azAhmiQEMcBZAKZLFIzu0D6HyfFzV2U"
    assert gsi.get_spreadsheet_id(arrow.Arrow(2022, 6, 1)) == "1etyjl4GU0KZZ7hpU8XucnenEhNnA94OTLTJDBaA3D-c"
    assert gsi.get_spreadsheet_id(arrow.Arrow(2022, 5, 1)) == "1VXQeSp4dITFISjtOw0L5utEWog5ug9xzkVn4OMJg7z8"
    print("For Jan 27th:")
    session_data = gsi.get_session_data(bad_pay.time_machine(arrow.Arrow(2023, 1, 27)))
    print(session_data)
    names = bad_pay.clean_name_list(session_data["Col A"])
    print(names)
    assert len(names) == 33
    print("For Jan 20th:")
    session_data = gsi.get_session_data(bad_pay.time_machine(arrow.Arrow(2023, 1, 20)))
    print(session_data)
    names = bad_pay.clean_name_list(session_data["Col A"])
    print(names)
    assert len(names) == 33
    assert session_data["Amount Charged"] == 4.91
    assert session_data["In Attendance"] == 33
    assert session_data["Courts"] == 6
    assert "Josy" in names
    assert "Raam" not in names


def test_sign_up_list():
    message = bad_pay.generate_sign_up_message(bp_test_inputs.wa_quick_fire_msgs)
    print(message)
    lines = message.split("\n")
    assert len(lines) == 51
    assert f"{bad_pay.time_machine(arrow.now().shift(days=7)).format('dddd, Do MMMM YYYY')}, 19:30 - 21:30:" in lines[0]
    assert lines[4] == "1. James (Host)"
    assert lines[36] == "33. Sean"
    assert "Sixtine\n" in message   # double: two attendees  signed up
    assert "Bia\n" in message       # in a single incoming message
    assert lines[-1] == "(copy and paste, adding your name to secure a spot)"
    message_1 = bad_pay.generate_sign_up_message(bp_test_inputs.wa_brief_messages)
    print(message_1)
    lines_1 = message_1.split("\n")
    assert len(lines_1) == 44
    assert f"{bad_pay.time_machine(arrow.now().shift(days=7)).format('dddd, Do MMMM YYYY')}, 19:30 - 21:30:" in lines[0]
    assert lines_1[4] == "1. James (Host)"
    assert lines_1[14] == "11. krystle"
    assert lines_1[-1] == "(copy and paste, adding your name to secure a spot)"
    without_host = bad_pay.generate_sign_up_message(
        bp_test_inputs.sample_with_extra_messages, host="")
    print(without_host)
    assert "James (Host)" not in without_host
    assert "1. Kevin k\n" in without_host
    without_extraneous_text = bad_pay.generate_sign_up_message(
        bp_test_inputs.sample_with_extra_messages, show_waitlist=False
    )
    print(without_extraneous_text)
    assert "Apologies" not in without_extraneous_text
    assert " make it" not in without_extraneous_text
    assert " Saurabh " in without_extraneous_text   # the "(X's friend)" case


def test_allocating_against_previous_sessions():
    copy_test_file_to_downloads("Statement Download 2022-Oct-20 19-46-16-THREE MONTHS.csv")
    oct_7th = arrow.Arrow(2022, 10, 7)
    sept_30th = oct_7th.shift(days=-7)
    for d in (sept_30th, oct_7th):
        bad_pay.set_session_date(bad_pay.time_machine(d))
        bad_pay.delete_session()
        bad_pay.monday_process()
    people_7th = bad_pay.get_current_session()["People"]
    assert round(people_7th["Jordan"]["transfer"], 2) == 4.91
    sept_30th_dt = bad_pay.time_machine(sept_30th).datetime
    sept_30th_people = coll.find_one({"Date": {"$eq": sept_30th_dt}})["People"]
    assert sept_30th_people
    assert "Ali I" in sept_30th_people
    expected_transfers_from = ("Jordan", "Jon L", "Levi", "Mara", "Shashank")
    assert all(round(sept_30th_people[p]["transfer"], 2) == 4.50
               for p in expected_transfers_from)
    clean_downloads_folder()


def test_multiple_session_processing():
    def run_process_for_session(date: arrow.Arrow, delete_existing: bool = True):
        bad_pay.set_session_date(bad_pay.time_machine(date))
        if delete_existing:
            bad_pay.delete_session()
        _ = input(f"Go ahead and run for {date.format('Do MMM')}?")
        bad_pay.monday_process()

    """31st Mar: all paid except Jon L"""
    copy_test_file_to_downloads("Statement Download 21Apr2023-1.csv")
    mar_31 = arrow.Arrow(2023, 3, 31)
    run_process_for_session(mar_31)
    unpaid_for_31st = bad_pay.get_unpaid()
    assert len(unpaid_for_31st) == 1
    assert "Jon" in unpaid_for_31st
    """first pass for 21st April is incomplete: three unpaid including Jon"""
    run_process_for_session(arrow.Arrow(2023, 4, 21))
    expected_unpaid = ["Ali", "Angela", "Jon"]
    unpaid_for_21st_apr = bad_pay.get_unpaid()
    assert len(unpaid_for_21st_apr) == len(expected_unpaid)
    assert all(name in unpaid_for_21st_apr for name in expected_unpaid)
    """second file completes payments for 21st Apr, 
        and includes Jon's overdue one for 31st Mar"""
    copy_test_file_to_downloads("Statement Download 21Apr2023-2.csv")
    run_process_for_session(arrow.Arrow(2023, 4, 21), delete_existing=False)
    assert not bad_pay.get_unpaid()
    assert bad_pay.get_current_session()["People"]["Jon"]["transfer"] == 4.50
    bad_pay.set_session_date(bad_pay.time_machine(mar_31))
    assert "Jon" not in bad_pay.get_unpaid()
    assert bad_pay.get_current_session()["People"]["Jon"]["transfer"] == 4.32
    """final file contains Ameya's payment for 28th, which was so early
        it was before the expected payment window for that session"""
    copy_test_file_to_downloads("Statement Download 2023-May-02 11-35-08.csv")
    run_process_for_session(arrow.Arrow(2023, 4, 28))
    assert "Ameya" in bad_pay.get_unpaid()
    run_process_for_session(arrow.Arrow(2023, 4, 21), delete_existing=False)
    bad_pay.set_session_date(bad_pay.time_machine(arrow.Arrow(2023, 4, 28)))
    assert "Ameya" not in bad_pay.get_unpaid()
    clean_downloads_folder()


def test_displaying_past_sessions():
    # bad_pay.show_session_details(bad_pay.get_current_session())
    print("")
    bad_pay.set_session_date(bad_pay.time_machine(arrow.Arrow(2023, 4, 14)))
    # bad_pay.delete_session()
    bad_pay.allow_reprocessing_of_previous_n_sessions(5)


def test_options_list():
    short_string = "short string"
    long_string = "hello " * 12
    options = [long_string] + [short_string] * 5
    print("")
    print(bad_pay.show_options_list(options))
    print(bad_pay.show_options_list([short_string, long_string[:68]]))
    test_long = long_string[:65]
    for extra_chars in range(10):
        test_long += f"{chr(65 + extra_chars)}"
        print(f"Iteration {extra_chars + 1}:")
        print(bad_pay.show_options_list([short_string, test_long, short_string]))


def test_one_spreadsheet_instead_of_monthly():
    data = gsi.get_session_data(bad_pay.time_machine(arrow.Arrow(2024, 1, 10)))
    print(data)


if __name__ == "__main__":
    # TODO: generate the next spreadsheet tab automatically?
    # TODO: for multi-person payments, record the name of person who paid?
    test_aug_19th()

