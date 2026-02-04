# This file is called 1st in the automated process that will update and send out the career director weekly placement report. 
# It calls the file "update-leadership-report.py", which updates the placement excel.
# This file then formats and prepares the email, and then sends it out to whoever should receive them.
# It will also update the BCC data team box folder titled Career Director Reports

import os
import smtplib
import ssl
from pathlib import Path
import mimetypes
from email.message import EmailMessage
from create_program_reports import main as build_program_report
from datetime import date
from dotenv import load_dotenv
from typing import Iterable
import calendar

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
OUTPATH_TEMPLATE = str(BASE_DIR / "WeeklyPlacement-{file_label}.xlsx")

# SMTP: Secure Mail Transfer Protocol. Allows us to send from the Pi
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER = os.getenv("SENDER")
APP_PASSWORD = os.getenv("SMTP_PASS")

# cc emails and bcc if wanted
CC_ADDRS = []
BCC_ADDRS = []

# box folder upload email
BOX_UPLOAD_EMAIL = os.getenv("BOX_UPLOAD_EMAIL")

# Nested program dictionary:  
# CAREER DIRECTOR -> (programs -> (list of programs), emails -> (list of emails))
program_dict = {
    "Name1": {
        "programs": ("BSAcc", "MAcc"),
        "emails": ("fake@fake.com", "fake2@fake.com"),
    },
    "Name2": {
        "programs": ("BSEDM",),
        "emails": ("fake@fake.com"),
    },
    "Name3": {
        "programs": ("BSEnt", "BSHRM", "BSStrat"),
        "emails": ("fake@fake.com"),
    },
    "Name4": {
        "programs": ("BSFin",),
        "emails": ("fake@fake.com"),
    },
    "Name5": {
        "programs": ("BSGSCM", "BSMgt"),
        "emails": ("fake@fake.com", "fake2@fake.com"),
    },
    "Name6": {
        "programs": ("BSIS", "MISM"),
        "emails": ("fake@fake.com", "fake2@fake.com"),
    },
    "Name7": {
        "programs": ("BSMktg",),
        "emails": ("fake@fake.com", "fake2@fake.com"),
    },
    "Name8": {
        "programs": ("MBA",),
        "emails": ("fake@fake.com", "fake2@fake.com"),
    },
    "Name9": {
        "programs": ("MPA",),
        "emails": ("fake@fake.com", "fake2@fake.com"),
    },
}

# Formats the list of programs to appear nicely in the email subject line
def program_to_subjectHeader(programs):
    if not programs:
        raise RuntimeError("No programs: check dictionary")
    if len(programs) == 1:
        return programs[0]
    if len(programs) > 1:
        return ", ".join(programs)

# Formats the list of programs to appear nicely for each excel file name
def program_to_filename(programs):
    if not programs:
        raise RuntimeError("No programs: check dictionary")
    if len(programs) == 1:
        return programs[0]
    if len(programs) > 1:
        return "-".join(programs)

# This is where the message is built: set the subject and the content
def build_message(filepath, to_addrs: Iterable[str], contact_name, subj_label, a):
    msg = EmailMessage()
    msg["From"] = SENDER
    msg["To"] = ", ".join(to_addrs)
    if CC_ADDRS:
        msg["Cc"] = ", ".join(CC_ADDRS)

    if a in (1,2):
        msg["Subject"] = f"{subj_label} Month End Placement Report {date.today():%m-%d-%Y}"
        body = (
f"Good Morning {contact_name},\n\n"
"Today is the last day of the month. The report has been updated with our current, month-end placement statistics.\n\n"
"The first excel sheet tab contains the placement totals for the entire Class of 2026. "
"Each sheet after that contains data for your program or programs.\n"
"The sheets include a 'Most Recent Friday' table, which shows the data as of the day and time you received this email.\n"
"There is also a 'Weekly History' table, which provides a picture of how the placement numbers have been changing over the past couple of months.\n"
"Each table shows how many students are in each placement category. The most important categories are bolded. At the bottom of each table, you can see the placement percentage\n"
"If you have any questions, please contact the BCC Data Team. If there are any discrepancies in the data, please let us know.\n\n"
"A visualization of this month end data will be sent out within the next business day. It will have a monthly placement historical data comparison.\n\n"
"Sincerely,\n"
"BCC Data Team"      
        )
    else:
        msg["Subject"] = f"{subj_label} Weekly Placement Report {date.today():%m-%d-%Y}"
        body = (
f"Good Morning {contact_name},\n\n"
"Here are the updated placement and internship reports for your programs from the past week.\n\n"
"The first excel sheet tab contains the placement totals for the entire Class of 2026. "
"Each sheet after that contains data for your program or programs.\n"
"The sheets include a 'Most Recent Friday' table, which shows the data as of the day and time you received this email.\n"
"There is also a 'Weekly History' table, which provides a picture of how the placement numbers have been changing over the past couple of months.\n"
"Each table shows how many students are in each placement category. The most important categories are bolded. At the bottom of each table, you can see the placement percentage\n"
"If you have any questions, please contact the BCC Data Team. If there are any discrepancies in the data, please let us know.\n\n"
"Sincerely,\n"
"BCC Data Team"
    )
    msg.set_content(body)
    attach_file(msg, filepath)
    return msg

# Builds the BOX message
def build_box(filepath):
    msg = EmailMessage()
    msg["Subject"] = f"Auto upload: weekly_overview {date.today():%Y-%m-%d}"
    msg["From"]    = SENDER
    msg["To"]      = BOX_UPLOAD_EMAIL
    msg.set_content("Automated upload from Raspberry Pi.")
    attach_file(msg, filepath)
    return msg

# Attaches the file to the email
def attach_file(msg, path):
    if not Path(path).exists():
        raise FileNotFoundError(f"Attachment not found: {path}")
    ctype, _ = mimetypes.guess_type(path)
    if not ctype:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype=maintype,
            subtype=subtype,
            filename=os.path.basename(path),
        )

# Sends the email
def send(smtp, msg, to_addrs: Iterable[str]):
    seen, all_rcpts = set(), []
    for a in to_addrs:
        if a and a not in seen:
            seen.add(a)
            all_rcpts.append(a)
    smtp.send_message(msg, from_addr=SENDER, to_addrs=all_rcpts)

# The crontab runs this script every day. This checks if the script should run today, and how it should run depending on the day.
def run_check(today, is_monthend):
    is_friday = today.weekday() == 4
    if is_monthend and is_friday:
        return 2
    elif is_monthend:
        return 1
    elif is_friday:
        return 0
    else:
        return None
    

# Connects to the SMTP server -> parses through dictionary -> update the proper excel -> builds the emails -> sends the emails -> repeats for each career director
def mainflow():
    if not APP_PASSWORD:
        raise RuntimeError("SMTP_PASS not set")
    if not BOX_UPLOAD_EMAIL:
        raise RuntimeError("BOX_UPLOAD_EMAIL not set")

    today = date.today()
    last_day = calendar.monthrange(today.year, today.month)[1]
    is_monthend = today.day == last_day

    a = run_check(today, is_monthend)
    if a is None:
        print("Not Friday or month-end; exiting...")
        return
    
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.ehlo()
        s.starttls(context=context)
        s.ehlo()
        s.login(SENDER, APP_PASSWORD)

        for contact_name, data in program_dict.items():
            programs = data["programs"]
            subj_label = program_to_subjectHeader(programs)
            file_label = program_to_filename(programs)
            emails = data["emails"]

            filename = OUTPATH_TEMPLATE.format(file_label=file_label)
            os.environ["OUTPUT_PATH"] = filename

            build_program_report(programs)

            message = build_message(filename, emails, contact_name, subj_label, a)
            box_msg = build_box(filename)

            send(s, box_msg, [BOX_UPLOAD_EMAIL])
            human_envelope = list(emails) + CC_ADDRS + BCC_ADDRS
            send(s, message, human_envelope)
            
            print(f"Email sent to {contact_name}! And Box Updated")


if __name__=="__main__":
    mainflow()
