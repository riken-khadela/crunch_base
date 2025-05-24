from update_scrapper.scrapper import MainUpdateScrapping1
from new_scrapper.scrapper import MainNewScrapping
import threading


def run_scrapper1():
    scrapper = MainUpdateScrapping1()
    scrapper.thread_function()

# def run_scrapper2():
#     scrapper = MainNewScrapping()
#     scrapper.thread_function()

if __name__ == "__main__":
    t1 = threading.Thread(target=run_scrapper1)
    # t2 = threading.Thread(target=run_scrapper2)

    t1.start()
    # t2.start()

    t1.join()
    # t2.join()