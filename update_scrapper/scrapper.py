from update_scrapper.summery import SUMMARY
from update_scrapper.finance import FINANCIAL
from update_scrapper.news import NEWS
import update_scrapper.settings as cf
import threading, os, time
from datetime import datetime
from update_scrapper.investment import INVESTMENT
from logger import CustomLogger
from concurrent.futures import ThreadPoolExecutor, as_completed
from catch_coockies import CatchCookies

number_of_records = 20
number_of_threads = 4

class MainUpdateScrapping1(SUMMARY, FINANCIAL, NEWS, INVESTMENT, cf.main_setting):
    def __init__(self):
        super().__init__()
        self.logger = CustomLogger(log_file_path="log/update.log")
        self.session_manager = CatchCookies()

    def main(self, url, dict):
        summary = self.summary_process_logic(url, dict)
        financialurl = summary[1]
        newsurl = summary[2]
        investmenturl = summary[3]

        financial = {}
        news = {}
        investment_section = {}

        if financialurl:
            financial = self.financial_process_logic(financialurl, dict)

        if investmenturl:
            investment_section = self.investment_process_logic(investmenturl, dict)

        if newsurl:
            news = self.news_process_logic(newsurl, dict)

        org_detail = {}
        if summary[0] and 'organization_url' in summary[0]:
            org_detail.update(summary[0])
            org_detail["is_updated"] = 1
            org_detail["update_timestamp"] = datetime.now()
            if financial.get('financial'):
                org_detail.update(financial)
            if investment_section.get('investment'):
                org_detail.update(investment_section)
            if news.get('news'):
                org_detail.update(news)

        return org_detail

    def thread_logic(self, all_documents):
        alldetails = []
        update_urls = []
        dicts = all_documents

        def process_and_append(url, dict_data):
            try:
                details = self.main(url, dict_data)
                if 'summary' in details:
                    return details, dict_data
            except Exception as e:
                self.logger.error(f"[ERROR] Exception in thread: {e}")
                return None, None

        with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
            future_to_data = {executor.submit(process_and_append, d['organization_url'], d): d for d in dicts}
            for future in as_completed(future_to_data):
                try:
                    result, update_data = future.result()
                    if result and 'organization_url' in result:
                        alldetails.append(result)
                        update_urls.append(update_data)
                except Exception as e:
                    self.logger.error(f"[ERROR] Processing failed: {e}")

        alldetails = [details for details in alldetails if 'organization_url' in details]
        if len(alldetails) > 0:
            try:
                cf.update_crunch_detail(alldetails)
            except cf.DuplicateKeyError as e:
                self.logger.error("Skipping duplicate records:", e)

    def thread_function(self):
        while True:
            try:
                self.session_manager.refresh_session()
                dicts = self.read_crunch_details(number_of_records)
                if not dicts:
                    for _ in range(10):
                        self.logger.info("No documents found, sleeping...")
                        time.sleep(60)
                        dicts = self.read_crunch_details(number_of_records)
                        if dicts:
                            break
                self.thread_logic(dicts)
                self.logger.log(f"Wating for a minitue restart the process of scrapping after completed scrapping a batch size of {number_of_records}")
                time.sleep(60)
            except Exception as e:
                self.logger.error(f"[ERROR] Exception in main thread loop: {e}")
