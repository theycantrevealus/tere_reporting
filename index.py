""" OPERATION CENTRALIZED MONITORING TOOLS """
# import time
import sys
import multiprocessing
import curses
from dateutil import parser
import pandas as pd
from modules.report_fact_detail import ReportFactDetail
# from modules.logging import Logging, LoggingType

class CursorPosition:
    """ Shell cursor """
    def __init__(self, x, y):
        self.x = x
        self.y = y

class Unbuffered(object):
    """ Clear buffer """
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        """ What should I said ??? """
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, datas):
        """ What should I said ??? """
        self.stream.writelines(datas)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)

class Main:
    """ Main application class """
    def __init__(self):
        sys.stdout = Unbuffered(sys.stdout)

        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()

        # Initialize all tool class
        # self.logger = Logging()
        self.option_caption = [
            'Quit',
            'Application Manual',
            'Database        - Back Up',
            'Report [MANUAL] - Fact Detail',
            'Report [MANUAL] - DCI 0POIN',
            'Report [AUTO]   - Fact Detail',
            'Report [AUTO]   - DCI 0POIN',
            'Multiprocess'
        ]
        self.result_lists = [{'title': str(i)} for i in self.option_caption]

        self.queue = multiprocessing.Queue()


    def run(self):
        """ RUN """
        while True:
            self.stdscr.clear()
            self.show_menu()
            self.stdscr.refresh()

            # titlenumber = int(self.reader())
            titlenumber = self.get_number()

            if titlenumber != 0:
                userchoice = titlenumber
                if(userchoice > len(self.result_lists) - 1):
                    self.run()
                else:
                    self.printer(f"You choose: {str(userchoice)}) {str(self.result_lists[userchoice]['title'])}")
                    response = self.get_yes_no()
                    if(response == 'y'):
                        p = multiprocessing.Process(target=self.background_processor(userchoice), args=(self.queue,))
                        p.start()
                        p.join()
                        # self.printer(self.queue.get())
                        self.printer('Press any key to continue.(Y)')

                    else:
                        self.printer('Press any key to continue.(N)')

                    self.stdscr.getch()
            else:
                self.printer('Quit application')
                break

            self.stdscr.refresh()


    def show_menu(self):
        """ Show application menu """
        header = [
            "*********************************************************",
            "**  Welcome to OPERATION CENTRALIZED MONITORING TOOLS  **",
            "*********************************************************"
        ]
        i = 0
        while i < len(self.option_caption):
            selected= f"{str(i)} ) {self.result_lists[i]['title']}"
            header.append(selected)
            i += 1

        header.append("*********************************************************\n")

        self.stdscr.addstr(0, 0, "\n".join(header))


    def background_processor(self, target):
        """ Background process manager """
        # Switch target to function
        self.queue.put(f"Processing {str(self.result_lists[target]['title'])} on background...")
        if(target == 3):
            self.generate_fact_detail("2024-10-15")


    def get_yes_no(self):
        """ Confirmation """
        while True:
            self.printer("Enter (y/n): ")
            self.stdscr.refresh()
            user_input = self.stdscr.getstr().decode().lower()
            if user_input in ['y', 'n']:
                return user_input
            self.printer("Invalid input. Please enter 'y' or 'n'.")
            self.stdscr.refresh()
            # self.stdscr.getch()

    def get_number(self):
        """ Listen input """
        curses.echo()
        result = ""
        while True:
            self.printer_l(f"Enter number ({result}): ")
            self.stdscr.refresh()
            c = self.stdscr.getch()
            if c == ord('\n'):
                if(result == ''):
                    self.stdscr.addstr(self.current_cursor().y, 0, " " * 80)
                else:
                    return int(result)
            elif c == 127:  # Backspace
                result = result[:-1]
            elif c >= 48 and c <= 57:  # 0-9
                result += chr(c)
            self.stdscr.addstr(self.current_cursor().y, 0, " " * 80)
            self.stdscr.refresh()

    # Main Function
    def generate_fact_detail(self, parse_date: str):
        """ Manual Fact Detail """
        date_obj = pd.to_datetime(parse_date)
        last_day = date_obj - pd.Timedelta(days=1)

        from_date = parser.isoparse(f'{last_day.strftime("%Y-%m-%d")}T17:00:00.000Z')
        to_date = parser.isoparse(f'{parse_date}T17:00:00.000Z')

        fact_detail = ReportFactDetail()
        fact_detail.produce_data(from_date,to_date)

    # Utility
    def printer_l(self, word):
        """ Custom print for cursor current line """
        self.stdscr.addstr(self.current_cursor().y, 0, f"{word}")

    def printer(self, word):
        """ Custom print for cursor """
        self.stdscr.addstr(self.current_cursor().y + 1, 0, f"{word}")

    def reader(self):
        """ Custom reader for cursor """
        return self.stdscr.getstr(self.current_cursor().y + 1, 0).decode()

    def current_cursor(self):
        """ Cursor definition """
        y, x = self.stdscr.getyx()
        return CursorPosition(x, y)

    def cleanup(self):
        """ Cursor cleanup """
        curses.nocbreak()
        self.stdscr.keypad(False)
        curses.echo()
        curses.endwin()


if __name__ == "__main__":
    main = Main()
    # main.run()

    try:
        main.run()
    finally:
        main.cleanup()