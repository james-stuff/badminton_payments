import googleapiclient.errors
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os
import time


scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]
cred_path = "C:\\Users\\j_a_c\\Python Stuff\\_google_credentials\\"
token_file = f"{cred_path}token.json"
cred_file = [fn for fn in os.listdir(cred_path) if fn.startswith("client_secret_")][0]
if os.path.exists(token_file) and time.time() > os.path.getmtime(token_file) + (60 * 60 * 24 * 7):
    os.remove(token_file)
if os.path.exists(token_file):
    creds = Credentials.from_authorized_user_file(token_file, scopes)
else:
    flow = InstalledAppFlow.from_client_secrets_file(f"{cred_path}{cred_file}", scopes)
    creds = flow.run_local_server(port=0)
    with open(token_file, 'w') as token:
        token.write(creds.to_json())
sheets_service = build('sheets', 'v4', credentials=creds)


def get_session_data(session_date) -> dict:
    tab = session_date.day
    if session_date.year > 2023:
        tab = f"{tab} {session_date.format('MMM')}"
    try:
        sheet_data = sheets_service.spreadsheets().values().get(
            spreadsheetId=get_spreadsheet_id(session_date),
            range=f"{tab}!A1:K49").execute()
    except googleapiclient.errors.HttpError:
        print(f"Hmmm . . . there doesn't seem to be a Sheet for "
              f"{session_date.format('Do MMMM YYYY')}")
        return {}
    all_values = sheet_data["values"]
    feb_2023_format = all_values[0][1] != "Cash"
    courts_col, attendance_row, amount_row, start_taking_names = (0, 1, 2, 8) \
        if feb_2023_format else (3, 35, 36, 0)
    app_data = {
        "Col A": [v[0] if v else "" for v in all_values[start_taking_names:]],
        "Courts": int(all_values[0][courts_col]),
        "In Attendance": int(all_values[attendance_row][0]),
        "Amount Charged": float(all_values[amount_row][3][1:]),
    }
    return app_data


def get_spreadsheet_id(session_date) -> str:
    drive_service = build('drive', 'v3', credentials=creds)
    listing = drive_service.files().list(
        q="mimeType='application/vnd.google-apps.spreadsheet'",
        pageSize=100, fields="nextPageToken, files(id, name)").execute()
    files = listing["files"]
    date_formats = tuple(f"{'M' * n} YYYY" for n in (3, 4))
    for f in files:
        if session_date.year > 2023 and f["name"] == "Badminton Payments":
            return f["id"]
        for df in date_formats:
            if session_date.format(df) in f["name"]:
                return f["id"]
    return ""


def create_new_session_sheet(session_date, court_rate):
    """Creates blank sheet for the next session, also creating a new
        containing workbook, if one doesn't already exist"""
    destination_ss = get_spreadsheet_id(session_date)
    if not destination_ss:
        book_title = "Badminton Payments" if session_date.year > 2023 \
            else session_date.format("MMM YYYY")
        new_spreadsheet = sheets_service.spreadsheets().create(
            body={"properties": {"title": book_title}},
            fields='spreadsheetId'
        ).execute()
        destination_ss = new_spreadsheet.get('spreadsheetId')

    # copy template into destination sheet
    new_sheet_id = sheets_service.spreadsheets().sheets().copyTo(
        spreadsheetId="1UXxnh7r9yu21uxfSIO5BnjQseCVhP5ZUXFrLQ-OFKQw",
        sheetId=1154866216,
        body={"destinationSpreadsheetId": destination_ss}
    ).execute()["sheetId"]

    # rename the newly pasted sheet with the day of the session
    sheet_title = session_date.format("D")
    if session_date.year > 2023:
        sheet_title += f' {session_date.format("MMM")}'
    rename_request_body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": new_sheet_id,
                        "title": sheet_title
                    },
                    "fields": "title"
                }
            }
        ]
    }
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=destination_ss,
        body=rename_request_body
    ).execute()

    # clear stuff
    sheets_service.spreadsheets().values().batchClear(
        spreadsheetId=destination_ss,
        body={"ranges": [f"{sheet_title}!A9:A49", f"{sheet_title}!D9:J49"]}
    ).execute()

    # set certain cells/ranges to desired initial values
    batch_update_body = {
        "data": [
            {
                # number of courts
                "range": f"{sheet_title}!A1:A1", "values": [[6]]
            },
            {
                # court rate in force
                "range": f"{sheet_title}!G1:G1", "values": [[court_rate]]
            },
            {
                # payment checkboxes
                "range": f"{sheet_title}!B9:C41", "values": [[False] * 2] * 33
            },
            {
                # cash received
                "range": f"{sheet_title}!B5:B5", "values": [[0.00]]
            },
        ],
        "valueInputOption": "USER_ENTERED",
    }
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=destination_ss,
        body=batch_update_body
    ).execute()

    # TODO: put new sheet in its correct place in the order and make it default
    #       (if necessary, could do what I used to do manually,
    #       i.e. take a copy of the first sheet, overwrite the original and rename
    # TODO: maybe also fill in the Transfer check-boxes in Monday process?
    # TODO: similar thing to court rates in force, but for shuttle levy
