""" OPERATION CENTRALIZED MONITORING TOOLS """
import time
import sys
# import multiprocessing
import curses
# import pandas as pd
# from dateutil import parser
# from modules.report_fact_detail import ReportFactDetail
from modules.logging import Logging, LoggingType

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
        self.logger = Logging()
        self.option_caption = [
            'Quit',
            'Application Manual',
            'Database        - Back Up',
            'Report [MANUAL] - Fact Detail',
            'Report [MANUAL] - DCI 0POIN',
            'Report [AUTO]   - Fact Detail',
            'Report [AUTO]   - DCI 0POIN'
        ]
        self.result_lists = [{'title': str(i)} for i in self.option_caption]
        # self.show_menu()

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
                        self.printer('Press any key to continue.(Y)')

                        #                 p = multiprocessing.Process(target=self.background_processor)
                        #                 p.daemon = True  # Set as daemon
                        #                 p.start()

                    else:
                        self.printer('Press any key to continue.(N)')

                    self.stdscr.getch()
                    self.printer('Press any key to continue.')
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


    def background_processor(self):
        """ Background process manager """
        while True:
            time.sleep(1)


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