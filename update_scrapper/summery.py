import requests
requests.packages.urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import update_scrapper.settings as cf
from update_scrapper.settings import main_setting
from logger import CustomLogger
import base64

logger = CustomLogger(log_file_path="log/update.log")

class SUMMARY(main_setting):
    def __init__(self):
        self.About={}
        self.Details ={}
        self.M_AND_A_DETAILS={}
        self.Sub_Organizations={}
        
        
    def get_request(self,search):
        logger.log('searching for : '+search)
        #print('searching for : '+search)
        # proxies=cf.proxies()
        proxies = cf.getProxies()
        c = 0
        while True:
            try:
                # getHeaders = {
                #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
                # }
                # res = requests.get(search, headers=getHeaders, timeout=30,proxies=proxies)

                ### New Code with Scrapedo proxy ####
                logger.log(f"############# Searching: {search} #########")
                res = requests.get(
                                    url=search,
                                    proxies=proxies,
                                    verify=False
                                )
                if res.status_code == 200:
                    #Return Response with status True
                    logger.log(f"Success - {search}")
                    return True, res
                else:
                    logger.log(f"Failed - {search} with result code {res.status_code}")
                logger.log(res.status_code)
                
            except Exception as e:
                logger.warning(e)
            time.sleep(0.5)
            logger.log("Retrying again for:" + str(search)+"Checking try again:" + str(c))
            proxies = cf.getProxies()
            c = c + 1
            if c > 5: 
                logger.log(f"Failed - {search} - No more retries")
                break
        return False, False
    
    def about_section(self,information,main_data ,old_dict={}):
        
        old_about = old_dict.get('about', {})   
        old_About = {
        "location": old_about.get('location', ''),
        "no_of_employees": old_about.get('no_of_employees', ''),
        "last_funding_type": old_about.get('last_funding_type', ''),
        "ipo_status": old_about.get('ipo_status', ''),
        "website": old_about.get('website', ''),
        "cb_rank": old_about.get('cb_rank', ''),
        "acquired_by": old_about.get('acquired_by', ''),
        "investor_type": old_about.get('investor_type', ''),
        "founded_at" : old_about.get('founded_at', ''),
        "social_media" : old_about.get('social_media', {}),
        "growth_score" : old_about.get('growth_score', 0),
        "cb_rank" : old_about.get('cb_rank', 0),
        "heat_score" : old_about.get('heat_score', 0),
    }

        try :
            information_sec2 = information.find('div',{'class':'overview-row'})
            if information_sec2 :
                for i in   information_sec2.find_all('label-with-icon'): 
                    
                    # founded at
                    if i.get('iconkey') == "icon_event" :
                        old_About['founded_at'] = i.find('span').text.strip()
                
                    # location
                    if i.get('iconkey') == "icon_location" :
                        old_About['location'] = i.find('span').text.strip()
                
                    # ipo_status
                    if i.get('iconkey') == "icon_flag" :
                        old_About['ipo_status'] = i.find('span').text.strip()
                
                    # no of employees
                    if i.get('iconkey') == "icon_people_three" :
                        old_About['no_of_employees'] = i.find('span').text.strip()

                    # company website
                    if i.get('iconkey') == "icon_external_link" :
                        old_About['website'] = i.find('span').text.strip()
                
                    # last_funding_type
                    if i.get('iconkey') == "icon_company" :
                        old_About['last_funding_type'] = i.find('span').text.strip()
                
                    # aquired by
                    if i.get('iconkey') == "icon_acquisition" :
                        identifier_aquisition = i.find('field-formatter').find('identifier-formatter')
                        if identifier_aquisition :
                            old_About['acquired_by'] = identifier_aquisition.text.strip()

                # last finding type
                # cb rank
                # investor typen
                
            
            # social
            social_links = information_sec2.find("span",{'class' : 'social-link-icons'})
            
            fb_social = social_links.find('a',{"aria-label" : "View on Facebook" })
            if fb_social :
                old_About['social_media']['facebook'] = fb_social.get('href').lower().strip()
                
            twitter_social = social_links.find('a',{"aria-label" : "View on Twitter" })
            if twitter_social :
                old_About['social_media']['twitter'] = twitter_social.get('href').lower().strip()
                
            linkdin_social = social_links.find('a',{"aria-label" : "View on LinkedIn" })
            if linkdin_social :
                old_About['social_media']['linkedIn'] = linkdin_social.get('href').lower().strip()
            
            # score-and-trend
            for score in information.find_all('score-and-trend') : 
                score_split = score.text.strip().split(' ')
                if len(score_split) > 1:
                    if score_split[-1].isdigit() :
                        if "Growth Score" in score.text :
                            old_about['growth_score'] = int(score.text.strip().split(' ')[-1])
                        if "CB Rank" in score.text :
                            old_about['cb_rank'] = int(score.text.strip().split(' ')[-1])
                        if "Heat Score" in score.text :
                            old_about['heat_score'] = int(score.text.strip().split(' ')[-1])
        except Exception as e: print(f"ERROR in summery about_section : {e}")
        
        return old_About
    
    def details_section(self,information,main_data,old_dict):
        
        old_details_dict = old_dict.get('details', {})
        old_Details = {
            "industries": old_details_dict.get('industries', ''),
            "founded_date": old_details_dict.get('founded_date', ''),
            "operating_status": old_details_dict.get('operating_status', ''),
            "also_known_as": old_details_dict.get('also_known_as', ''),
            "hub_tags": old_details_dict.get('hub_tags', ''),
            "headquarter_regions": old_details_dict.get('headquarter_regions', ''),
            "founders": old_details_dict.get('founders', ''),
            "last_funding_type": old_details_dict.get('last_funding_type', ''),
            "stock_symbol": old_details_dict.get('stock_symbol', ''),
            "related_hubs": old_details_dict.get('related_hubs', ''),
            "company_type": old_details_dict.get('company_type', ''),
            "legal_name": old_details_dict.get('legal_name', ''),
            "email": old_details_dict.get('email', ''),
            "phone": old_details_dict.get('phone', ''),
            "social_media": {
                "facebook": old_details_dict.get('social_media', {}).get('facebook', ''),
                "twitter": old_details_dict.get('social_media', {}).get('twitter', ''),
                "linkedIn": old_details_dict.get('social_media', {}).get('linkedIn', '')
            },
            "sub_organization_of": old_details_dict.get('sub_organization_of', ''),
            "investor_type": old_details_dict.get('investor_type', ''),
            'description': old_details_dict.get('description', '')
        }
       
        try :
        
            # industries
            industries_data = self.main_data.find('chips-container')
            if industries_data:
                industries = [industry.text for industry in industries_data.find_all('a')]
                new_Industries=', '.join(industries)
                if new_Industries and new_Industries != old_Details['industries']:
                    old_Details['industries'] = new_Industries
            
            # other company details 
            company_details1 = information.find('overview-details')
            if company_details1 :
                company_group = company_details1.find('div',{"class":"group"})
            else :
                company_group = None
            if company_group :
                
                for title_field in company_group.find_all('tile-field') : 
                    
                    # legal name
                    if "legal name" in title_field.find('label-with-info').text.strip().lower() :
                        legal_name = title_field.find('blob-formatter')
                        if legal_name:
                                old_Details['legal_name'] = legal_name.text.strip()
                                
                    # also known as
                    if "also known as" in title_field.find('label-with-info').text.strip().lower() :
                        also_known_as = title_field.find('field-formatter').find('span')
                        if also_known_as:
                                old_Details['also_known_as'] = also_known_as.text.strip()
                    # operating_status
                    if "operating status" in title_field.find('label-with-info').text.strip().lower() :
                        new_Operating_Status = title_field.find('field-formatter').find('span')
                        if new_Operating_Status:
                                old_Details['operating_status'] = new_Operating_Status.text.strip()
                            
                    # Founders 
                    if "founders" in title_field.find('label-with-info').text.strip().lower() :
                        founders = title_field.find('field-formatter').find('span')
                        if founders:
                                old_Details['founders '] = founders.text.strip()
                            
                    # company_type 
                    if "company type" in title_field.find('label-with-info').text.strip().lower() :
                        company_type = title_field.find('field-formatter').find('span')
                        if company_type:
                                old_Details['company_type '] = company_type.text.strip()
                            
                    # Stock Symbol 
                    if "Stock Symbol" in title_field.find('label-with-info').text.strip().lower() :
                        company_type = title_field.find('field-formatter').find('span')
                        if company_type:
                                old_Details['stock_symbol '] = company_type.text.strip()
                            
                    # hub tags
                    # old_Details['hub_tags']
                    
                    # headquarter_regions
                    # old_Details['headquarter_regions']
                    
            # if company_details1 :
            #     company_description = company_details1.find('div',{"class":"description ng-star-inserted"})
            #     if company_description :
            #         company_description = company_description.find('tile-description')
            #         old_Details['description'] = company_description.text.strip()
                    
            # contact details
            if company_details1 :
                company_contact_if_details = company_details1.find_all('div',{"class":"group"})[-1]
                if company_contact_if_details :
                    for contact_detail in company_contact_if_details.find_all('tile-field') :
                        contact_detail_perent = contact_detail.find('label-with-info')
                        if 'phone number' in contact_detail_perent.text.strip().lower():
                                old_Details['phone'] = contact_detail.find('field-formatter').find('span').text.strip()
                                
                        if 'email' in contact_detail.text.strip().lower():
                                old_Details['email'] = contact_detail.text.strip()

            # sub_organization_of  old_Details['sub_organization_of']
            # investor_type  old_Details['investor_type']
            # related_hubs  old_Details['related_hubs']
            
        except Exception as e: print(f"ERROR in summery details_section : {e}")
       
       
        return old_Details
    
    def m_and_a_details_section(self,information,main_data, old_dict = {}):
        old_m_and_a_details_dict = old_dict.get('m_&_a_details', {})
        old_M_AND_A_DETAILS = {
            "transcation_name": old_m_and_a_details_dict.get('transcation_name', ''),
            "announced_date": old_m_and_a_details_dict.get('announced_date', ''),
            "acquired_by": old_m_and_a_details_dict.get('acquired_by', ''),
            "price": old_m_and_a_details_dict.get('price', '')
        }
        try:
                    
            details=information.find('mat-card',{'id':'acquired_by'})
            if details :
                if "M&A Details" in details.text :
                    for title_feild in details.find_all('tile-field'):
                        label_with_info = title_feild.find('label-with-info')
                        if title_feild.find('field-formatter') :
                            title_feild = title_feild.find('field-formatter')
                            if title_feild :
                        
                                if "Transaction Name" in label_with_info.text :
                                    if title_feild.find('field-formatter') :
                                        old_M_AND_A_DETAILS['transcation_name'] = title_feild.find('field-formatter').text.strip()
                                
                                if "Acquired by" in label_with_info.text :
                                    if title_feild.find('field-formatter') :
                                        old_M_AND_A_DETAILS['acquired_by'] = title_feild.find('field-formatter').text.strip()
                                
                                if "Announced Date" in label_with_info.text :
                                    if title_feild.find('field-formatter') :
                                        old_M_AND_A_DETAILS['announced_date'] = title_feild.find('field-formatter').text.strip()
                    
        except Exception as e: print(f"ERROR in summery m_and_a_details_section : {e}")
        
        return old_M_AND_A_DETAILS
    
    def sub_organizations_section(self,information,main_data,old_dict):
        # Check if 'sub_organizations' key exists in old_dict
        old_sub_organizations_dict = old_dict.get('sub_organizations', {})

        old_Sub_Organizations = {
            "number_of_sub_organization": old_sub_organizations_dict.get('number_of_sub_organization', ''),
            "subsidiary": old_sub_organizations_dict.get('subsidiary', ''),
            "product_data": old_sub_organizations_dict.get('product_data', {}),
        }
        try :

            table = information.find('table')
            rows = []
            if table :
                headers = [header.text.strip() for header in table.find_all('th')]
                for row in table.find_all('tr'): 
                    columns = row.find_all('td')
                    row_data = {headers[i]: row.find_all('td')[i].text.strip() for i in range(len(row.find_all('td')))}
                    row_data = {headers[i]: columns[i].text.strip() for i in range(len(columns))}
                    if row_data :
                        rows.append(row_data)
                old_Sub_Organizations['product_data'] = rows
                old_Sub_Organizations['number_of_sub_organization'] = len(rows)
            
        except Exception as e: print(f"ERROR in summery sub_organizations_section : {e}")
        
        return old_Sub_Organizations  
        
    def summary_process_logic(self,url,dict):
        old_dict = dict.get('summary', {})
        summary_detail={}
        self.About={}
        self.Details ={}
        self.overview_section={}
        self.M_AND_A_DETAILS={}
        self.Sub_Organizations={} 
        financial_url=''
        signals_and_news_url=''
        investment_url=''
        organization_logo = ''
        organization_name = ''
        session_id, cookies = self.load_session()
        isloaded, res = self.get_scrpido_requests(url, session_id, cookies)
        if isloaded:
            try :
                file_name = "crunchbase_logged_in.html"
                with open(file_name, '+w') as f:f.write(res.text)
                data = BeautifulSoup(res.text, 'lxml')
                self.data = data
                self.main_data = data
                org_type=dict.get('category', "Organization")
                new_organization_name=data.find('span',{'class':'entity-name'})
                if new_organization_name :
                    new_organization_name = new_organization_name.text.strip()
                    organization_name = dict.get('organization_name',{})
                    if new_organization_name and new_organization_name != organization_name:
                        organization_name = new_organization_name
                
                new_organization_logo=data.find('profile-v3-header-logo')
                if new_organization_logo:
                    new_organization_logo = new_organization_logo.find('source')
                    if new_organization_logo :
                        new_organization_logo = new_organization_logo.get('srcset').split(' ')[0]
                        organization_logo = dict.get('organization_logo',{})
                        if new_organization_logo and new_organization_logo != organization_logo:
                            organization_logo = new_organization_logo
                    
                About_section = data.find('profile-v3-header')
                if About_section :
                    self.About=self.about_section(About_section,data,old_dict)
                    
                overview_section = data.find('section',{"id":"overview"})
                if overview_section:
                    self.Details=self.details_section(overview_section,data,old_dict)

                m_and_a_section = data.find('section',{"id":"financials"})
                if m_and_a_section:
                    self.M_AND_A_DETAILS=self.m_and_a_details_section(m_and_a_section,data,old_dict)

                product_service = data.find('section',{'id':'predictions_and_insights'})
                Sub_Organizations_idx = 1
                if product_service :
                    for product_service_ in product_service.find_all('profile-column-layout') : 
                        if product_service_.find('header') and product_service_.find('header').text  == "Products and Services":
                            section_data = self.sub_organizations_section(product_service_, data, old_dict)
                            if section_data:
                                self.Sub_Organizations[str(Sub_Organizations_idx)] = section_data
                                Sub_Organizations_idx += 1
                        
                summary_detail={
                            "category":org_type,
                            "url_id": dict.get('url_id',{}),
                            "organization_url" :url,
                            "organization_name": organization_name,
                            "organization_logo": organization_logo,
                            "summary": {
                            "about":self.About,
                            "details": self.Details ,
                            "m_&_a_details": self.M_AND_A_DETAILS,
                            "sub_organizations": self.Sub_Organizations
                            }
                        }
                check_finacial=data.find('mat-nav-list',{'role':'navigation'})
                if check_finacial :
                    check_finacial = check_finacial.find_all('a',{'class' : 'link-detail'})
                    
                for financials_link in check_finacial: 
                    
                    if 'financial_details' in financials_link.get('href'):
                        financial_url=financials_link.get('href')
                        
                    if 'news_and_analysis' in financials_link.get('href'):
                        signals_and_news_url=financials_link.get('href')
                    
                    if 'Investments' in financials_link.get('href'):
                        investment_url=financials_link.get('href')
            except Exception as e: print(f"ERROR in summery summary_process_logic : {e}")
                        
        return summary_detail,financial_url,signals_and_news_url,investment_url    

