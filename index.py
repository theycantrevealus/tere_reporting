""" OPERATION CENTRALIZED MONITORING TOOLS """
import time
import sys
import multiprocessing
# import curses
# import pandas as pd
# from dateutil import parser
# from modules.report_fact_detail import ReportFactDetail
from modules.logging import Logging, LoggingType

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

        # self.stdscr = curses.initscr()
        # self.stdscr.refresh()
        # self.stdscr.clear()
        # curses.noecho()
        # curses.cbreak()

        # Initialize all tool class
        self.logger = Logging()

        self.i = 0
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
        self.show_menu()
        while self.i < len(self.option_caption):
            selected= f"{str(self.i)} ) {self.result_lists[self.i]['title']}"
            print (selected)
            self.i += 1


        while True:
            try:
                titlenumber = int(input('Enter number of the menu you want to choose: '))
                if titlenumber != 0:
                    userchoice=titlenumber
                    print('You choose: ' + str(userchoice) + ') ' + str(self.result_lists[userchoice]['title']))
                    if self.confirmation('Continue process? [y/n]: '):

                        p = multiprocessing.Process(target=self.background_processor)
                        p.daemon = True  # Set as daemon
                        p.start()

                        continue
                    else:
                        continue
                else:
                    print('Quit application')
                    break

            except ValueError:
                continue

        print('See you later!', flush=True)

    def show_menu(self):
        """ Show application menu """
        print("*********************************************************")
        print("**  Welcome to OPERATION CENTRALIZED MONITORING TOOLS  **")
        print("*********************************************************")


    def confirmation(self, prompt):
        """ Confirmation """
        while True:
            answer = input(prompt).upper()
            if answer == 'Y':
                return True
            if answer == 'N':
                return False
            print('Invalid')

    def background_processor(self):
        """ Background process manager """
        print("")
        while True:
            time.sleep(1)

    # def cleanup(self):
    #     """ What should I said ??? """
    #     curses.nocbreak()
    #     self.stdscr.keypad(False)
    #     curses.echo()
    #     curses.endwin()


if __name__ == "__main__":
    main = Main()
    main.run()

    # try:
    #     main.run()
    # finally:
    #     main.cleanup()