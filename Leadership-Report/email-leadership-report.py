# This file is called 1st in the automated process that will update and send out the overview weekly placement report. 
# It calls the file "update-leadership-report.py", which updates the placement excel.
# This file then formats and prepares the email, and then sends it out to whoever should receive them.
# It will also update the BCC data team box folder titled Weekly Reports

import os
from pathlib import Path
import smtplib, ssl, mimetypes
from email.message import EmailMessage
from datetime import date
import calendar

from update_overall_report import main as create_reports


# SMTP: Secure Mail Transfer Protocol. Creating a connection to the gmail SMTP server allows us to send emails from the Pi
# These are the required global variables that will get used in this script
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587  # STARTTLS
SENDER = os.getenv("SENDER")
APP_PASSWORD = os.getenv("SMTP_PASS")

# EMAIL ADDRESSES that will be the recipients of the email. I always send it to the bcc data team email so we can fact check and ensure it actually sent.
# Kept in .env for security
def env_list(name: str, default=None):
    raw = os.getenv(name)
    if raw is None:
        return default if default is not None else []
    return [e.strip() for e in raw.split(",") if e.strip()]

TO_ADDRS  = env_list("TO_ADDRS")
CC_ADDRS  = env_list("CC_ADDRS")
BCC_ADDRS = env_list("BCC_ADDRS")

# This is the Box Folder upload emails, we send copies there for backups
MAIN_BOX_UPLOAD_EMAIL = os.getenv("MAIN_BOX_UPLOAD_EMAIL")
MONTHEND_BOX_UPLOAD_EMAIL = os.getenv("MONTHEND_BOX_UPLOAD_EMAIL")

# This is the path to the excel sheet
BASE_DIR = Path(__file__).resolve().parent
OUTPATH1 = os.getenv("OUTPUT_PATH", str(BASE_DIR / "weekly_placement_report.xlsx")) # report that needs to be updated

# This function creates the email packet that will be sent every WEEK. This is where you create a subject and set the content/body of the email
def build_weekly_message(filepath):
    msg = EmailMessage()
    msg["Subject"] = f"Weekly Placement Report {date.today():%m-%d-%Y}"
    msg["From"] = SENDER
    msg["To"] = ", ".join(TO_ADDRS)
    if CC_ADDRS:
        msg["Cc"] = ", ".join(CC_ADDRS)

    msg.set_content("""
Good Morning Everyone,  
        
This is the Weekly Placement Report that will automatically be sent out every week on Friday at 10am.
It automatically pulls the new data from the database, updates the excel sheet, and then sends out the email.
It provides current, weekly updates so you can track how the BCC or a particular program is progressing.
        
The excel file contains 5 pages:
                    
  - SUMMARY: An overview of each programs placement data in a combined table. This allows us to spot trends in the BCC as a whole
  - TOTAL: The placement data for every student in the 2026 class in one table. The second table will add a new column every week with the new numbers so we can see the week to week change.
  - BY PROGRAM: Breaks the data out for each individual program. It also provides the week to week comparison for each program
  - TOTAL - Internship: The internship placement data in one table. The second table will add a new column every week with the new numbers so we can see the week to week change.
  - BY PROGRAM - Internship: Breaks the internship data out for each individual program. It also provides the week to week comparison for each program

            
If you notice any discrepencies in the data, please contact the data team.
            
Sincerely,
BCC Data Team
"""
    )
    attach_file(msg, filepath)
    return msg

# This function creates the email packet that will be sent at MONTH END. This is where you create a subject and set the content/body of the email
def build_monthly_message(filepath):
    msg = EmailMessage()
    msg["Subject"] = f"Month End Placement Report {date.today():%m-%d-%Y}"
    msg["From"] = SENDER
    msg["To"] = ", ".join(TO_ADDRS)
    if CC_ADDRS:
        msg["Cc"] = ", ".join(CC_ADDRS)

    msg.set_content("""
Good Morning Everyone,  
        
Today is the last day of the month. The report has been updated with our current, month-end placement statistics.
This report automatically pulls the new data from the database, updates the excel sheet, and then sends out this email.
It provides current, weekly and monthly updates so you can track how the BCC or a particular program is progressing.
        
The excel file contains 5 pages:
                    
  - SUMMARY: An overview of each programs placement data in a combined table. This allows us to spot trends in the BCC as a whole
  - TOTAL: MSB Class of 2026 placement infromation. The second table will add a new column every week with the new numbers so we can see the week to week change.
  - BY PROGRAM: Breaks placement data out for each program. It also provides the week to week comparison for each program
  - TOTAL - Internship: MSB internship placement data in one table. The second table will add a new column every week with the new numbers so we can see the week to week change.
  - BY PROGRAM - Internship: Breaks internship data out for each program. It also provides the week to week comparison for each program

            
If you notice any discrepencies in the data, please contact the data team.
                    
A visualization of this month end data will be sent out within the next business day. It will have a monthly placement historical data comparison.
            
Sincerely,
BCC Data Team
"""
    )
    attach_file(msg, filepath)
    return msg

# Creates an email that is sent to the main BOX folder. The BOX will upload the attached file.
def build_box_main(filepath):
    msg = EmailMessage()
    msg["Subject"] = f"Auto upload: weekly_placement_report {date.today():%Y-%m-%d}"
    msg["From"]    = SENDER
    msg.set_content("Automated upload from Raspberry Pi.")
    attach_file(msg, filepath)
    return msg

# This function attaches the report to the emails
def attach_file(msg, path):
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

# Sends out the emails to all of the recipients
def send(smtp, msg, to_addrs):
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


# Flow is as follows: check day -> update reports -> build messages -> start STMP connection -> send to BOX -> send out email
def mainflow():
    if not APP_PASSWORD:
        raise RuntimeError("SMTP_PASS not set")
    
    today = date.today()
    last_day = calendar.monthrange(today.year, today.month)[1]
    is_monthend = today.day == last_day

    a = run_check(today, is_monthend)
    if a is None:
        print("Not Friday or month-end; exiting...")
        return

    create_reports()

    if a == 0:
        message = build_weekly_message(OUTPATH1)
        box_upload_email = MAIN_BOX_UPLOAD_EMAIL
    elif a in (1,2):
        message = build_monthly_message(OUTPATH1)
        box_upload_email = MONTHEND_BOX_UPLOAD_EMAIL

    main_box_msg = build_box_main(OUTPATH1)
    main_box_msg["To"] = box_upload_email


    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.ehlo()
        s.starttls(context=context)
        s.ehlo()
        s.login(SENDER, APP_PASSWORD)

        send(s, main_box_msg, [box_upload_email])

        human_envelope = TO_ADDRS + CC_ADDRS + BCC_ADDRS
        send(s, message, human_envelope)

    print("Email sent! And Box Uploaded")
    

if __name__=="__main__":
    mainflow()
