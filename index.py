""" OPERATION CENTRALIZED MONITORING TOOLS """
import pandas as pd
from dateutil import parser
from modules.report_fact_detail import ReportFactDetail

i = 0

option_caption = [
    'Quit',
    'Application Manual',
    'Database        - Back Up',
    'Report [MANUAL] - Fact Detail',
    'Report [MANUAL] - DCI 0POIN',
    'Report [AUTO]   - Fact Detail',
    'Report [AUTO]   - DCI 0POIN'
]

result_lists = [{'title': str(i)} for i in option_caption]
print("*********************************************************")
print("**  Welcome to OPERATION CENTRALIZED MONITORING TOOLS  **")
print("*********************************************************")
while i < len(option_caption):
    NUMBER_LIST = str(i) + ') '
    stitle= NUMBER_LIST + '' + result_lists[i]['title']
    print (stitle)
    i+=1
print()

def generate_fact_detail(parse_date: str):
    """ Report Generate Fact Detail """
    date_obj = pd.to_datetime(parse_date)
    last_day = date_obj - pd.Timedelta(days=1)

    from_date = parser.isoparse(f'{last_day.strftime("%Y-%m-%d")}T17:00:00.000Z')
    to_date = parser.isoparse(f'{parse_date}T17:00:00.000Z')

    fact_detail = ReportFactDetail()
    fact_detail.produce_data(from_date,to_date)

def get_yes_or_no(prompt):
    """ Confirmation """
    while True:
        answer = input(prompt).upper()
        if answer == 'Y':
            return True
        if answer == 'N':
            return False
        print('Invalid')

while True:
    try:
        titlenumber = int(input('Enter number of the menu you want to choose: '))
        if not titlenumber == 0:
            userchoice=titlenumber
            print('You choose: ' + str(userchoice) + ') ' + str(result_lists[userchoice]['title']))
            if get_yes_or_no('Continue process? [y/n]: '):
                print('Yes')
                break
            else:
                print('No')
                continue
        else:
            print('Quit application')
            break

    except ValueError:
        continue

print('See you later!', flush=True)