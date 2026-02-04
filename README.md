# Placement-Reporting
## Report Overview
I created these reports as part of my work at the Business Career Center. I saved the data team hours of work every week by automating the placement updates and sending out emails. To do this, I used a python script that handles each step. 

Here is how the reports generally run
1) Scheduled to run every day using a crontab. Script checks whether it should run on that day. If not, it exits
2) Connects to the BYU Student DB using mysql.connector and runs the queries
3) Accesses Pre-formated Excel sheets and updates the appropriate tables/sheets using openpyxl
4) Sends out reports to emails using smtplib

## Leadership Reports
The leadership reports are geared towards the leadership in the Business Career Center. They present a comprhensive view of the placement statistics for the entier Marriot School of Business (MSB). It gives them totals and also a break down by program. This saves the Data Team a couple hours of work every week by sending out this email every Friday. It also runs on Month-Ends to give an end-of-month statistic.

## Career Director Reports
Each Career Director is in charge of 1 or more programs. These reports present placement information for their individual programs, along with a view of the MSB total. They can then compare whether they are above or below this average, and also see how many students still need help placing. 
