import requests
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from bs4 import BeautifulSoup 
import pandas as pd
import logging

from tqdm import tqdm

from edgar_db import EDGARDBClient



class EDGARAPIClient:
    """
    A client for interacting with the SEC's EDGAR API and website.
    
    Note: The SEC requires a User-Agent header with your company name and email.
    """
    
    EDGAR_URL = "https://www.sec.gov"
    API_URL = "https://data.sec.gov"
    
    def __init__(self, user_agent: str, log_file: str = "edgar_failures.log"):
        """
        Initialize the EDGAR client.
        
        Args:
            user_agent: User agent string in format "CompanyName email@example.com"
            log_file: Path to log file for all messages
        """
        # Setup logging
        self.logger = logging.getLogger('EDGARAPIClient')
        self.logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers (prevents duplicate logs)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        # File handler for ALL messages
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Changed from WARNING to DEBUG
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Console handler for general info
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_formatter = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        
        if not user_agent or "@" not in user_agent:
            raise ValueError("User agent must include company name and email address")
        
        self.headers = {
            "User-Agent": user_agent,
            "Content-Type":'application/json'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.rate_limit_delay = 0.2

    
    def _rate_limit(self):
        """Enforce rate limiting to comply with SEC guidelines."""
        time.sleep(self.rate_limit_delay)

    
    def get_company_cik(self, ticker: str) -> Optional[str]:
        self._rate_limit()
        
        url = f"{self.EDGAR_URL}/files/company_tickers.json"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            
            for company in data.values():
                if company.get("ticker", "").upper() == ticker.upper():
                    cik = str(company.get("cik_str", ""))
                    return cik.zfill(10)  # Pad CIK to 10 digits
            
            return None
        except Exception as e:
            print(f"Error searching ticker list: {e}")
            return None

    
    def get_company_submissions(self, cik: str) -> Dict:
        """
        Get all submissions for a company.
        
        Args:
            cik: Company CIK number
            
        Returns:
            Dictionary containing company information and filings
            
        Raises:
            ValueError: If CIK is invalid
            requests.exceptions.HTTPError: If API request fails
            requests.exceptions.RequestException: For other request errors
        """
        try:
            self._rate_limit()
            
            # Ensure CIK is properly formatted (10 digits with leading zeros)
            cik = str(cik).zfill(10)
            
            url = f"{self.API_URL}/submissions/CIK{cik}.json"
            
            response = self.session.get(url)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ValueError(f"Company with CIK {cik} not found") from e
            else:
                raise requests.exceptions.HTTPError(
                    f"HTTP error occurred while fetching submissions for CIK {cik}: {e}"
                ) from e
                
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(
                f"Request failed for CIK {cik}: {e}"
            ) from e
            
        except Exception as e:
            raise Exception(
                f"Unexpected error fetching submissions for CIK {cik}: {e}"
            ) from e

    
    def get_company_facts(self, cik: str) -> Dict:
        """
        Get company facts (XBRL data) for a company.
        
        Args:
            cik: Company CIK number
            
        Returns:
            Dictionary containing company facts
        """
        self._rate_limit()
        
        cik = str(cik).zfill(10)
        url = f"{self.API_URL}/api/xbrl/companyfacts/CIK{cik}.json"
        
        response = self.session.get(url)
        response.raise_for_status()
        
        return response.json()

    
    def search_filings(
        self, 
        cik: str, 
        form_type: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Search for filings by company and optionally filter by form type and date.
        
        Args:
            cik: Company CIK number
            form_type: Form type (e.g., "10-K", "10-Q", "8-K")
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame of filings matching the criteria
        """
        submissions = self.get_company_submissions(cik)
        filings = submissions.get("filings", {}).get("recent", {})
        
        # Convert parallel arrays to DataFrame
        df = pd.DataFrame({
            "accessionNumber": filings.get("accessionNumber", []),
            "filingDate": filings.get("filingDate", []),
            "reportDate": filings.get("reportDate", []),
            "form": filings.get("form", []),
            "primaryDocument": filings.get("primaryDocument", []),
            "primaryDocDescription": filings.get("primaryDocDescription", []),
        })
        
        # Return empty DataFrame if no data
        if df.empty:
            return df
        
        # Apply filters
        if form_type:
            df = df[df["form"] == form_type]
        
        if date_from:
            df = df[df["filingDate"] >= date_from]
            
        if date_to:
            df = df[df["filingDate"] <= date_to]
        
        # Reset index after filtering
        df = df.reset_index(drop=True)
        
        return df

    
    def _process_rss(self, enteries:List, date_check:datetime) -> Dict:
        holder = []
        check = False
        for entry in enteries:
            x = {
                'title': entry.find('title').text if entry.find('title') else None,
                'cik': entry.find('title').text.split('(')[1].split(')')[0] if entry.find('title') else None,
                'link': entry.find('link')['href'] if entry.find('link') else None,
                'accession_number': entry.find('id').text.split('=')[1] if entry.find('id') else None,
                'form': entry.find('category')['term'] if entry.find('category') else None,
                'updated': datetime.fromisoformat(entry.find('updated').text) if entry.find('updated') else None,
            }
            holder.append(x)
            if x['updated'].date() < date_check:
                check = True
        return holder, check

      
    def get_recent_form4_filings(self, days_back=5):
        """
        Fetch recent Form 4 filings
        
        Args:
            days_back: Number of days to look back
            max_filings: Maximum number of filings to retrieve
            
        Returns:
            DataFrame with filing information
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).date()
        start = 0
        count = 100  
        holder = []
        print(f"Fetching Form 4 filings...")
        while True:
            self._rate_limit()
            search_url = f"{self.EDGAR_URL}/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=4&owner=only&start={start}&count={count}&output=atom"        
            response = requests.get(search_url,headers=self.headers)
            response.raise_for_status()        
            soup = BeautifulSoup(response.content, "xml")
            entries = soup.find_all("entry")
            start += count
            processed_entries, check = self._process_rss(entries,cutoff_date)
            holder.extend(processed_entries)
            if check:
                break
        
        print(f"Recived: {len(holder)}")
        return holder

 
    def _get_xml_file(self,txt_url:str) -> str:
        self._rate_limit()
        response = requests.get(txt_url, headers=self.headers)
        response.raise_for_status()
        x = response.text
        output = x[x.find("<FILENAME>")+10:x.find(".xml")+4]
        return output 

        
    def process_form4_filing(self, filing_dict: Dict) -> Dict:
        """
        Process a Form 4 filing and extract transaction data.
        
        Args:
            filing_dict: Dictionary containing filing info (from get_recent_form4_filings)
            
        Returns:
            Dictionary with parsed Form 4 data
        """
        try:
            self._rate_limit()
            
            accession = filing_dict['accession_number'].replace('-', '')
            cik = filing_dict['cik']
            base_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}"
            xml_file = self._get_xml_file(f"{base_url}/{filing_dict['accession_number']}.txt")
            
            
            
            working_url = f"{base_url}/{xml_file}"
            
            
            response = requests.get(working_url, headers=self.headers)
            response.raise_for_status()
            
            # Parse the XML
            soup = BeautifulSoup(response.content, 'xml')
            
            # Extract key information
            form4_data = {
                'accession_number': filing_dict['accession_number'],
                'cik': cik,
                'title': filing_dict['title'],
                'filing_date': filing_dict['updated'],
                
                # Issuer information
                'issuer_cik': soup.find('issuerCik').text if soup.find('issuerCik') else None,
                'issuer_name': soup.find('issuerName').text if soup.find('issuerName') else None,
                'issuer_ticker': soup.find('issuerTradingSymbol').text if soup.find('issuerTradingSymbol') else None,
                
                # Reporting owner information
                'owner_cik': soup.find('rptOwnerCik').text if soup.find('rptOwnerCik') else None,
                'owner_name': soup.find('rptOwnerName').text if soup.find('rptOwnerName') else None,
                'is_director': soup.find('isDirector').text if soup.find('isDirector') else None,
                'is_officer': soup.find('isOfficer').text if soup.find('isOfficer') else None,
                'is_ten_percent_owner': soup.find('isTenPercentOwner').text if soup.find('isTenPercentOwner') else None,
                'officer_title': soup.find('officerTitle').text if soup.find('officerTitle') else None,
                
                # Transactions
                'transactions': []
            }
            
            # Extract non-derivative transactions
            non_derivative_txns = soup.find_all('nonDerivativeTransaction')
            for txn in non_derivative_txns:
                transaction = {
                    'security_title': txn.find('securityTitle').find('value').text if txn.find('securityTitle') else None,
                    'transaction_date': txn.find('transactionDate').find('value').text if txn.find('transactionDate') else None,
                    'transaction_code': txn.find('transactionCode').text if txn.find('transactionCode') else None,
                    'shares': txn.find('transactionShares').find('value').text if txn.find('transactionShares') else None,
                    'price_per_share': txn.find('transactionPricePerShare').find('value').text if txn.find('transactionPricePerShare') else None,
                    'acquired_disposed': txn.find('transactionAcquiredDisposedCode').find('value').text if txn.find('transactionAcquiredDisposedCode') else None,
                    'shares_owned_following': txn.find('sharesOwnedFollowingTransaction').find('value').text if txn.find('sharesOwnedFollowingTransaction') else None,
                    'transaction_type': 'non-derivative'
                }
                form4_data['transactions'].append(transaction)
            
            # Extract derivative transactions
            derivative_txns = soup.find_all('derivativeTransaction')
            for txn in derivative_txns:
                transaction = {
                    'security_title': txn.find('securityTitle').find('value').text if txn.find('securityTitle') else None,
                    'transaction_date': txn.find('transactionDate').find('value').text if txn.find('transactionDate') else None,
                    'transaction_code': txn.find('transactionCode').text if txn.find('transactionCode') else None,
                    'shares': txn.find('transactionShares').find('value').text if txn.find('transactionShares') else None,
                    'price_per_share': txn.find('transactionPricePerShare').find('value').text if txn.find('transactionPricePerShare') else None,
                    'acquired_disposed': txn.find('transactionAcquiredDisposedCode').find('value').text if txn.find('transactionAcquiredDisposedCode') else None,
                    'underlying_shares': txn.find('sharesOwnedFollowingTransaction').find('value').text if txn.find('sharesOwnedFollowingTransaction') else None,
                    'transaction_type': 'derivative'
                }
                form4_data['transactions'].append(transaction)
            
            return form4_data
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"✗ HTTP error fetching: {accession}")
            return None
        except Exception as e:
            self.logger.error(f"✗ Error processing Form 4: {accession} - {e}")
            return None


    def process_and_store_form4s(self, filings_list: List[Dict], db_client: 'EDGARDBClient') -> Dict:
        """
        Process multiple Form 4 filings and store them in the database.
        
        Args:
            filings_list: List of filing dictionaries from get_recent_form4_filings
            db_client: EDGARDBClient instance for database operations
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'total_filings': len(filings_list),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_transactions': 0,
            'failed_accessions': []
        }
        
        self.logger.info(f"Starting batch processing of {len(filings_list)} Form 4 filings")
        
        for i, filing in enumerate(filings_list, 1):
            if i % 10 == 0:
                print(f"Processed {i}/{len(filings_list)} filings")
            accession = filing.get('accession_number', 'UNKNOWN')
            
            try:
                # Process the Form 4 filing
                form4_data = self.process_form4_filing(filing)
                
                if not form4_data:
                    stats['failed'] += 1
                    stats['failed_accessions'].append({
                        'accession': accession,
                        'reason': 'Processing failed'
                    })
                    continue
                
                # Check if filing already exists in database
                existing = db_client.get_filing_by_accession(form4_data['accession_number'])
                if existing:
                    stats['skipped'] += 1
                    continue
                
                # Insert into database
                if db_client.insert_form4_filing(form4_data):
                    stats['successful'] += 1
                    stats['total_transactions'] += len(form4_data.get('transactions', []))
                    self.logger.info(f"✓ Stored {accession} - {form4_data.get('issuer_ticker', 'N/A')} - {len(form4_data.get('transactions', []))}")
                else:
                    stats['failed'] += 1
                    stats['failed_accessions'].append({
                        'accession': accession,
                        'reason': 'Database insertion failed'
                    })
                    self.logger.error(f"✗ Failed to store {accession} in database")
                    
            except Exception as e:
                stats['failed'] += 1
                stats['failed_accessions'].append({
                    'accession': accession,
                    'reason': str(e)
                })
                self.logger.error(f"Error processing filing {accession}: {e}", exc_info=True)
        
        # Log summary
        self.logger.info(f"Batch processing complete:")
        self.logger.info(f"  Total: {stats['total_filings']}")
        self.logger.info(f"  Successful: {stats['successful']}")
        self.logger.info(f"  Skipped: {stats['skipped']}")
        self.logger.info(f"  Failed: {stats['failed']}")
        self.logger.info(f"  Total transactions: {stats['total_transactions']}")
        
        if stats['failed_accessions']:
            self.logger.warning(f"Failed accessions: {stats['failed_accessions']}")
        
        return stats

        
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    edgar_company=os.getenv("edgar_company")
    edgar_email=os.getenv("edgar_email")
    edgar = EDGARAPIClient(user_agent=f"{edgar_company} {edgar_email}")
    db = EDGARDBClient("edgar_filings.db")

    filings = edgar.get_recent_form4_filings(days_back=1)
    stats = edgar.process_and_store_form4s(filings, db)
    
    
    