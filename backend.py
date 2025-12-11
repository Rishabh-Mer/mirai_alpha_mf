"""
Mutual Fund Factsheet Extraction Backend
=========================================
Extracts structured data from mutual fund factsheet PDFs.
Handles multi-page tables, normalizes fields across AMCs.
"""

import pdfplumber
import pandas as pd
import re
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import logging
from difflib import SequenceMatcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ISINMapper:
    """Maps company names to ISIN codes using a reference CSV file"""
    
    # Built-in ISIN database for major Indian stocks
    BUILTIN_ISIN_DB = {
        # Large Cap - Banking
        'hdfc bank': 'INE040A01034',
        'icici bank': 'INE090A01021',
        'state bank of india': 'INE062A01020',
        'sbi': 'INE062A01020',
        'axis bank': 'INE238A01034',
        'kotak mahindra bank': 'INE237A01028',
        'indusind bank': 'INE095A01012',
        'federal bank': 'INE171A01029',
        'bandhan bank': 'INE545U01014',
        'idfc first bank': 'INE092T01019',
        
        # Large Cap - IT
        'tata consultancy services': 'INE467B01029',
        'tcs': 'INE467B01029',
        'infosys': 'INE009A01021',
        'wipro': 'INE075A01022',
        'hcl technologies': 'INE860A01027',
        'tech mahindra': 'INE669C01036',
        'ltimindtree': 'INE214T01019',
        'coforge': 'INE591G01017',
        'persistent systems': 'INE262H01013',
        'mphasis': 'INE356A01018',
        
        # Large Cap - Financials
        'bajaj finance': 'INE296A01024',
        'bajaj finserv': 'INE918I01018',
        'hdfc life insurance': 'INE795G01014',
        'sbi life insurance': 'INE123W01016',
        'icici prudential life': 'INE726G01019',
        'cholamandalam investment': 'INE121A01024',
        'shriram finance': 'INE721A01013',
        'muthoot finance': 'INE414G01012',
        'angel one': 'INE732I01013',
        
        # Large Cap - Auto
        'maruti suzuki': 'INE585B01010',
        'tata motors': 'INE155A01022',
        'mahindra & mahindra': 'INE101A01026',
        'm&m': 'INE101A01026',
        'bajaj auto': 'INE917I01010',
        'hero motocorp': 'INE158A01026',
        'eicher motors': 'INE066A01021',
        'tvs motor company': 'INE494B01023',
        'ashok leyland': 'INE208A01029',
        'samvardhana motherson': 'INE775A01035',
        'samvardhana motherson international': 'INE775A01035',
        
        # Large Cap - Pharma
        'sun pharmaceutical': 'INE044A01036',
        'sun pharma': 'INE044A01036',
        'dr reddys laboratories': 'INE089A01023',
        'cipla': 'INE059A01026',
        'divi\'s laboratories': 'INE361B01024',
        'lupin': 'INE326A01037',
        'aurobindo pharma': 'INE406A01037',
        'biocon': 'INE376G01013',
        'mankind pharma': 'INE634S01028',
        'torrent pharmaceuticals': 'INE685A01028',
        
        # Large Cap - FMCG
        'hindustan unilever': 'INE030A01027',
        'hul': 'INE030A01027',
        'itc': 'INE154A01025',
        'nestle india': 'INE239A01016',
        'britannia industries': 'INE216A01030',
        'dabur india': 'INE016A01026',
        'marico': 'INE196A01026',
        'godrej consumer products': 'INE102D01028',
        'colgate palmolive': 'INE259A01022',
        'tata consumer products': 'INE192A01025',
        
        # Large Cap - Energy/Oil
        'reliance industries': 'INE002A01018',
        'ongc': 'INE213A01029',
        'oil india': 'INE274J01014',
        'indian oil corporation': 'INE242A01010',
        'ioc': 'INE242A01010',
        'bharat petroleum': 'INE029A01011',
        'bpcl': 'INE029A01011',
        'gail india': 'INE129A01019',
        
        # Large Cap - Telecom
        'bharti airtel': 'INE397D01024',
        'jio financial services': 'INE758E01017',
        'indus towers': 'INE121J01017',
        
        # Large Cap - Metals
        'tata steel': 'INE081A01020',
        'jsw steel': 'INE019A01038',
        'hindalco industries': 'INE038A01020',
        'vedanta': 'INE205A01025',
        'coal india': 'INE522F01014',
        'nmdc': 'INE584A01023',
        
        # Large Cap - Power
        'ntpc': 'INE733E01010',
        'power grid corporation': 'INE752E01010',
        'adani power': 'INE814H01011',
        'tata power': 'INE245A01021',
        'adani green energy': 'INE364U01010',
        
        # Large Cap - Industrials
        'larsen & toubro': 'INE018A01030',
        'l&t': 'INE018A01030',
        'siemens': 'INE003A01024',
        'abb india': 'INE117A01022',
        'bharat heavy electricals': 'INE257A01026',
        'bhel': 'INE257A01026',
        'cummins india': 'INE298A01020',
        
        # Mid Cap - Consumer Durables
        'titan company': 'INE280A01028',
        'titan': 'INE280A01028',
        'havells india': 'INE176B01034',
        'voltas': 'INE226A01021',
        'whirlpool of india': 'INE716A01013',
        'crompton greaves consumer': 'INE299U01018',
        'dixon technologies': 'INE935N01020',
        'dixon technologies (india)': 'INE935N01020',
        'amber enterprises': 'INE371P01015',
        'amber enterprises india': 'INE371P01015',
        'kalyan jewellers': 'INE303R01014',
        'kalyan jewellers india': 'INE303R01014',
        
        # Mid Cap - Real Estate
        'dlf': 'INE271C01023',
        'godrej properties': 'INE484J01027',
        'oberoi realty': 'INE093I01010',
        'prestige estates projects': 'INE811K01011',
        'prestige estates': 'INE811K01011',
        'phoenix mills': 'INE211B01039',
        'the phoenix mills': 'INE211B01039',
        'brigade enterprises': 'INE791I01019',
        'sobha': 'INE671H01015',
        'macrotech developers': 'INE670K01029',
        
        # Mid Cap - Capital Markets
        'bse': 'INE118H01025',
        'bse ltd': 'INE118H01025',
        'mcx': 'INE745G01035',
        'multi commodity exchange': 'INE745G01035',
        'multi commodity exchange of india': 'INE745G01035',
        'icici securities': 'INE763G01038',
        'cams': 'INE596I01012',
        'kfin technologies': 'INE138Y01010',
        'religare enterprises': 'INE621H01010',
        
        # Mid Cap - Aerospace & Defense
        'hindustan aeronautics': 'INE066F01020',
        'hal': 'INE066F01020',
        'bharat electronics': 'INE263A01024',
        'bel': 'INE263A01024',
        'bharat dynamics': 'INE171Z01018',
        'zen technologies': 'INE251B01027',
        'data patterns': 'INE822Q01010',
        'paras defence': 'INE114S01016',
        
        # Mid Cap - Electrical Equipment
        'cg power and industrial solutions': 'INE067A01029',
        'cg power': 'INE067A01029',
        'ge vernova t&d india': 'INE200A01026',
        'ge vernova': 'INE200A01026',
        'ge t&d india': 'INE200A01026',
        'suzlon energy': 'INE040H01021',
        'inox wind': 'INE066P01011',
        'premier energies': 'INE555Y01016',
        'waaree energies': 'INE377N01016',
        'apar industries': 'INE372A01015',
        'polycab india': 'INE455K01017',
        'kaynes technology': 'INE918Z01012',
        'kaynes technology india': 'INE918Z01012',
        
        # Mid Cap - Industrials
        'thermax': 'INE152A01029',
        'grindwell norton': 'INE536A01023',
        'titagarh rail systems': 'INE615H01020',
        'ptc industries': 'INE591D01018',
        
        # Mid Cap - Chemicals
        'asian paints': 'INE021A01026',
        'pidilite industries': 'INE318A01026',
        'upl': 'INE628A01036',
        'srf': 'INE647A01010',
        'gujarat fluorochemicals': 'INE538A01037',
        'navin fluorine': 'INE048G01026',
        'deepak nitrite': 'INE288B01029',
        
        # Mid Cap - Retail/Consumer
        'avenue supermarts': 'INE192R01011',
        'dmart': 'INE192R01011',
        'trent': 'INE849A01020',
        'trent ltd': 'INE849A01020',
        'zomato': 'INE758T01015',
        'swiggy': 'INE044G01017',
        'nykaa': 'INE388Y01029',
        'fsn e-commerce ventures': 'INE388Y01029',
        'devyani international': 'INE872J01015',
        'v2 retail': 'INE945K01017',
    }
    
    def __init__(self, csv_path: str = None):
        self.isin_map: Dict[str, str] = {}
        self.company_names: List[str] = []
        
        # Load built-in database first
        self.isin_map = self.BUILTIN_ISIN_DB.copy()
        
        # Then load from CSV if provided (CSV entries will override built-in)
        if csv_path and os.path.exists(csv_path):
            self._load_csv(csv_path)
    
    def _load_csv(self, csv_path: str):
        """Load ISIN mappings from CSV file"""
        try:
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding, skiprows=1)
                    break
                except UnicodeDecodeError:
                    continue
            
            # Find the security name and ISIN columns
            name_col = None
            isin_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if 'security' in col_lower or 'company' in col_lower or 'name' in col_lower:
                    name_col = col
                elif 'isin' in col_lower:
                    isin_col = col
            
            if name_col and isin_col:
                for _, row in df.iterrows():
                    name = str(row[name_col]).strip()
                    isin = str(row[isin_col]).strip()
                    
                    if name and isin and isin.startswith('INE'):
                        # Store with normalized name as key
                        normalized = self._normalize_name(name)
                        self.isin_map[normalized] = isin
                        self.company_names.append(name)
                
                logger.info(f"Loaded {len(self.isin_map)} ISIN mappings from {csv_path}")
            else:
                logger.warning(f"Could not find Security Name or ISIN columns in {csv_path}")
                
        except Exception as e:
            logger.error(f"Error loading ISIN CSV: {e}")
    
    def _normalize_name(self, name: str) -> str:
        """Normalize company name for matching"""
        # Remove common suffixes and normalize
        name = name.lower().strip()
        
        # Remove common suffixes
        suffixes = [
            ' limited', ' ltd', ' ltd.', ' pvt', ' private', 
            ' public', ' inc', ' corp', ' corporation',
            ' (india)', ' india', ' & co', ' co.'
        ]
        
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Remove special characters
        name = re.sub(r'[^\w\s]', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def _similarity(self, a: str, b: str) -> float:
        """Calculate string similarity ratio"""
        return SequenceMatcher(None, a, b).ratio()
    
    def get_isin(self, company_name: str, threshold: float = 0.8) -> str:
        """
        Get ISIN for a company name.
        Returns ISIN if found with high confidence, else returns '-'
        """
        if not company_name:
            return '-'
        
        normalized = self._normalize_name(company_name)
        
        # Exact match
        if normalized in self.isin_map:
            return self.isin_map[normalized]
        
        # Fuzzy match
        best_match = None
        best_score = 0
        
        for ref_name, isin in self.isin_map.items():
            score = self._similarity(normalized, ref_name)
            
            # Also check if one contains the other
            if normalized in ref_name or ref_name in normalized:
                score = max(score, 0.85)
            
            if score > best_score:
                best_score = score
                best_match = isin
        
        if best_score >= threshold:
            return best_match
        
        return '-'
    
    def batch_lookup(self, company_names: List[str]) -> Dict[str, str]:
        """Batch lookup ISINs for multiple companies"""
        return {name: self.get_isin(name) for name in company_names}


@dataclass
class HoldingRecord:
    """Represents a single holding/security record"""
    date: str
    amc: str
    fund_name: str
    security: str
    isin: str
    sector: str
    pct_of_aum: float
    market_value: Optional[float] = None  # Market value in Crores (calculated from AUM * weight)
    quantity: Optional[float] = None


class FactsheetExtractor:
    """
    Main extraction class for mutual fund factsheets.
    Handles various AMC formats and normalizes output.
    """
    
    # Common sector mappings for normalization
    SECTOR_MAPPINGS = {
        'it - software': 'Information Technology',
        'it-software': 'Information Technology',
        'information technology': 'Information Technology',
        'software': 'Information Technology',
        'banks': 'Banking',
        'banking': 'Banking',
        'private sector bank': 'Banking',
        'public sector bank': 'Banking',
        'finance': 'Financial Services',
        'financial services': 'Financial Services',
        'nbfc': 'Financial Services',
        'pharmaceuticals & biotechnology': 'Pharmaceuticals',
        'pharmaceuticals': 'Pharmaceuticals',
        'pharma': 'Pharmaceuticals',
        'healthcare': 'Healthcare',
        'consumer durables': 'Consumer Durables',
        'consumer goods': 'Consumer Goods',
        'fmcg': 'FMCG',
        'retailing': 'Retailing',
        'retail': 'Retailing',
        'auto components': 'Automobile',
        'automobiles': 'Automobile',
        'automobile': 'Automobile',
        'auto': 'Automobile',
        'telecom - services': 'Telecom',
        'telecom': 'Telecom',
        'telecommunications': 'Telecom',
        'realty': 'Real Estate',
        'real estate': 'Real Estate',
        'construction': 'Construction',
        'capital markets': 'Capital Markets',
        'chemicals & petrochemicals': 'Chemicals',
        'chemicals': 'Chemicals',
        'industrial products': 'Industrial Products',
        'industrial manufacturing': 'Industrial Manufacturing',
        'aerospace & defense': 'Aerospace & Defense',
        'defence': 'Aerospace & Defense',
        'electrical equipment': 'Electrical Equipment',
        'power': 'Power',
        'oil & gas': 'Oil & Gas',
        'energy': 'Energy',
        'metals & mining': 'Metals & Mining',
        'cement': 'Cement & Construction Materials',
    }
    
    # AMC name mappings
    AMC_MAPPINGS = {
        'motilaloswal': 'Motilal Oswal',
        'motilal oswal': 'Motilal Oswal',
        'hdfc': 'HDFC',
        'icici prudential': 'ICICI Prudential',
        'sbi': 'SBI',
        'axis': 'Axis',
        'kotak': 'Kotak',
        'nippon india': 'Nippon India',
        'aditya birla': 'Aditya Birla Sun Life',
        'dsp': 'DSP',
        'tata': 'Tata',
        'uti': 'UTI',
        'franklin templeton': 'Franklin Templeton',
        'mirae asset': 'Mirae Asset',
        'pgim': 'PGIM India',
        'invesco': 'Invesco India',
        'edelweiss': 'Edelweiss',
        'canara robeco': 'Canara Robeco',
        'bandhan': 'Bandhan',
        'quant': 'Quant',
        'parag parikh': 'PPFAS',
        'ppfas': 'PPFAS',
        'groww': 'Groww',
        'baroda bnp': 'Baroda BNP Paribas',
    }
    
    def __init__(self, isin_csv_path: str = None):
        self.extracted_data: List[HoldingRecord] = []
        
        # Auto-detect ISIN CSV path if not provided
        if isin_csv_path is None:
            # Look for ISIN CSV in common locations
            possible_paths = [
                os.path.join(os.path.dirname(__file__), 'data', 'isin_master.csv'),
                os.path.join(os.path.dirname(__file__), 'data', 'List_of_Companies.csv'),
                os.path.join(os.path.dirname(__file__), 'isin_master.csv'),
                os.path.join(os.path.dirname(__file__), 'List_of_Companies.csv'),
                'data/isin_master.csv',
                'data/List_of_Companies.csv',
                'isin_master.csv',
                'List_of_Companies.csv',
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    isin_csv_path = path
                    break
        
        self.isin_mapper = ISINMapper(isin_csv_path)
        
    def normalize_sector(self, sector: str) -> str:
        """Normalize sector names across AMCs"""
        if not sector:
            return "Unknown"
        sector_lower = sector.lower().strip()
        return self.SECTOR_MAPPINGS.get(sector_lower, sector.title())
    
    def normalize_amc(self, amc: str) -> str:
        """Normalize AMC names"""
        if not amc:
            return "Unknown"
        amc_lower = amc.lower().strip()
        for key, value in self.AMC_MAPPINGS.items():
            if key in amc_lower:
                return value
        return amc.title()
    
    def parse_percentage(self, value: str) -> float:
        """Parse percentage values from various formats"""
        if not value:
            return 0.0
        try:
            # Remove % sign and whitespace
            cleaned = str(value).replace('%', '').replace(',', '').strip()
            # Handle negative values
            if cleaned.startswith('(') and cleaned.endswith(')'):
                cleaned = '-' + cleaned[1:-1]
            return float(cleaned)
        except (ValueError, AttributeError):
            return 0.0
    
    def parse_number(self, value: str) -> Optional[float]:
        """Parse numeric values from various formats"""
        if not value:
            return None
        try:
            cleaned = str(value).replace(',', '').replace('`', '').replace('₹', '').strip()
            if cleaned == '-' or cleaned == '' or cleaned.lower() == 'na':
                return None
            return float(cleaned)
        except (ValueError, AttributeError):
            return None
    
    def extract_date_from_filename(self, filename: str) -> str:
        """Extract date from filename in format AMC_FundName_YYYYMM.pdf"""
        try:
            # Try to find YYYYMM pattern
            match = re.search(r'(\d{6})\.pdf$', filename, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                year = date_str[:4]
                month = date_str[4:6]
                return f"{year}-{month}-01"
            
            # Try YYYY-MM pattern
            match = re.search(r'(\d{4})[-_]?(\d{2})', filename)
            if match:
                return f"{match.group(1)}-{match.group(2)}-01"
            
            return datetime.now().strftime("%Y-%m-01")
        except Exception:
            return datetime.now().strftime("%Y-%m-01")
    
    def extract_amc_from_filename(self, filename: str) -> str:
        """Extract AMC name from filename"""
        try:
            # Remove extension and split
            base = os.path.splitext(filename)[0]
            parts = base.replace('_', ' ').split()
            if parts:
                return self.normalize_amc(parts[0])
            return "Unknown"
        except Exception:
            return "Unknown"
    
    def extract_fund_name_from_text(self, text: str) -> str:
        """Extract fund name from PDF text"""
        patterns = [
            r'([\w\s]+(?:Large\s*(?:and|&)?\s*Mid\s*Cap|Large\s*Cap|Mid\s*Cap|Small\s*Cap|Multi\s*Cap|Flexi\s*Cap)\s*Fund)',
            r'([\w\s]+Fund)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                fund_name = match.group(1).strip()
                # Clean up
                fund_name = re.sub(r'\s+', ' ', fund_name)
                if len(fund_name) > 10 and 'fund' in fund_name.lower():
                    return fund_name
        
        return "Unknown Fund"
    
    def extract_aum_from_text(self, text: str) -> Optional[float]:
        """Extract AUM (Assets Under Management) from PDF text in Crores"""
        # Common patterns for AUM in Indian mutual fund factsheets
        patterns = [
            # "Latest AUM (31-Dec-2024) ` 9,001.07 (` cr)"
            r'Latest\s+AUM[^`]*`\s*([\d,]+\.?\d*)\s*\(?[`₹]?\s*cr',
            # "AUM ₹ 9,001.07 Cr" or "AUM: ₹9,001 Cr"
            r'AUM[:\s]*[`₹]?\s*([\d,]+\.?\d*)\s*(?:Cr|Crore)',
            # "Net Assets: 9001.07 Crores"
            r'Net\s+Assets[:\s]*[`₹]?\s*([\d,]+\.?\d*)\s*(?:Cr|Crore)',
            # "Fund Size: ₹9,001 Cr"
            r'Fund\s+Size[:\s]*[`₹]?\s*([\d,]+\.?\d*)\s*(?:Cr|Crore)',
            # "Monthly AAUM ` 8,481.23 (` cr)"
            r'Monthly\s+AAUM[^`]*`\s*([\d,]+\.?\d*)\s*\(?[`₹]?\s*cr',
            # General pattern: number followed by cr/crore
            r'(\d{1,3}(?:,\d{3})*\.?\d*)\s*\(?[`₹]?\s*cr\)?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    aum_str = match.group(1).replace(',', '')
                    aum = float(aum_str)
                    # Sanity check - AUM should be reasonable (between 10 and 10,00,000 Cr)
                    if 10 <= aum <= 1000000:
                        return aum
                except (ValueError, IndexError):
                    continue
        
        return None

    def extract_portfolio_table(self, pdf_path: str) -> Tuple[List[Dict], Dict[str, str]]:
        """
        Extract portfolio holdings table from PDF.
        Returns tuple of (holdings_list, metadata_dict)
        """
        holdings = []
        metadata = {
            'amc': '',
            'fund_name': '',
            'date': '',
            'aum_cr': None,  # AUM in Crores
        }
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                all_text = ""
                all_tables = []
                
                # Extract text and tables from all pages
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text() or ""
                    all_text += page_text + "\n"
                    
                    # Extract tables
                    tables = page.extract_tables()
                    for table in tables:
                        if table and len(table) > 1:
                            all_tables.append({
                                'page': page_num + 1,
                                'data': table
                            })
                
                # Extract metadata from text
                metadata['fund_name'] = self.extract_fund_name_from_text(all_text)
                
                # Extract AUM
                metadata['aum_cr'] = self.extract_aum_from_text(all_text)
                
                # Try to extract date from text
                date_match = re.search(r'(?:as\s+on|data\s+as\s+on|as\s+of)[\s:]+(\d{1,2}[-/]\w+[-/]\d{2,4}|\d{1,2}\s+\w+\s+\d{4}|\d{2}[-/]\d{2}[-/]\d{4})', all_text, re.IGNORECASE)
                if date_match:
                    metadata['date'] = date_match.group(1)
                
                # Process tables to find portfolio holdings
                holdings = self._process_tables(all_tables, all_text)
                
        except Exception as e:
            logger.error(f"Error extracting from {pdf_path}: {str(e)}")
            
        return holdings, metadata
    
    def _process_tables(self, tables: List[Dict], full_text: str) -> List[Dict]:
        """Process extracted tables to identify and parse portfolio holdings"""
        holdings = []
        current_sector = "Unknown"
        
        # Keywords to identify portfolio/holdings tables
        portfolio_keywords = ['scrip', 'security', 'stock', 'holding', 'weightage', 'weight']
        
        for table_info in tables:
            table = table_info['data']
            if not table or len(table) < 2:
                continue
            
            # Try to identify header row
            header_row = None
            header_idx = 0
            is_two_column_format = False
            
            for idx, row in enumerate(table[:3]):  # Check first 3 rows for header
                if row:
                    row_text = ' '.join([str(cell).lower() for cell in row if cell])
                    if any(kw in row_text for kw in portfolio_keywords):
                        header_row = row
                        header_idx = idx
                        # Check if this is a two-column portfolio format (Scrip | Weight | Scrip | Weight)
                        scrip_count = row_text.count('scrip')
                        if scrip_count >= 2 or (len(row) >= 4 and 'scrip' in row_text):
                            is_two_column_format = True
                        break
            
            if not header_row:
                continue
            
            # Handle two-column portfolio format
            if is_two_column_format:
                holdings.extend(self._parse_two_column_table(table, header_idx))
            else:
                # Map column indices
                col_map = self._map_columns(header_row)
                
                if not col_map.get('security') and not col_map.get('scrip'):
                    continue
                
                # Process data rows
                for row in table[header_idx + 1:]:
                    if not row:
                        continue
                    
                    holding = self._parse_holding_row(row, col_map, current_sector)
                    if holding and holding.get('security'):
                        holdings.append(holding)
        
        # If no structured table found, try text extraction
        if not holdings:
            holdings = self._extract_holdings_from_text(full_text)
        
        return holdings
    
    def _parse_two_column_table(self, table: List, header_idx: int) -> List[Dict]:
        """Parse a two-column portfolio table (common in Indian MF factsheets)"""
        holdings = []
        
        for row in table[header_idx + 1:]:
            if not row or len(row) < 2:
                continue
            
            # Process left side (columns 0-1)
            if len(row) >= 2:
                security = str(row[0]).strip() if row[0] else None
                weight = row[1]
                
                if security and self._is_valid_security(security):
                    pct = self.parse_percentage(weight)
                    if pct > 0:
                        holdings.append({
                            'security': security,
                            'isin': '',
                            'sector': 'Unknown',
                            'pct_of_aum': pct,
                            'market_value': None,
                            'quantity': None,
                        })
            
            # Process right side (columns 2-3) if present
            if len(row) >= 4:
                security = str(row[2]).strip() if row[2] else None
                weight = row[3]
                
                if security and self._is_valid_security(security):
                    pct = self.parse_percentage(weight)
                    if pct > 0:
                        holdings.append({
                            'security': security,
                            'isin': '',
                            'sector': 'Unknown',
                            'pct_of_aum': pct,
                            'market_value': None,
                            'quantity': None,
                        })
        
        return holdings
    
    def _is_valid_security(self, name: str) -> bool:
        """Check if a string is a valid security name"""
        if not name:
            return False
        
        name_lower = name.lower().strip()
        
        # Skip headers and totals
        skip_patterns = [
            'total', 'grand total', 'equity', 'net receivables', 
            'scrip', 'security', 'stock', 'weightage', 'weight',
            'debt', 'cash', 'net current', 'receivables', 'payables',
            'equity & equity', 'related', 'holdings', 'portfolio'
        ]
        
        if any(pattern in name_lower for pattern in skip_patterns):
            return False
        
        # Must have reasonable length
        if len(name) < 3 or len(name) > 100:
            return False
        
        # Should contain at least some letters
        if not any(c.isalpha() for c in name):
            return False
        
        return True
    
    def _map_columns(self, header_row: List) -> Dict[str, int]:
        """Map column names to indices"""
        col_map = {}
        
        column_patterns = {
            'security': ['scrip', 'security', 'stock', 'name', 'holding', 'company'],
            'isin': ['isin'],
            'sector': ['sector', 'industry'],
            'pct_of_aum': ['weight', '%', 'pct', 'weightage', 'allocation', '% of', 'aum'],
            'market_value': ['market', 'value', 'mv', 'amount'],
            'quantity': ['quantity', 'qty', 'units', 'shares'],
        }
        
        for idx, cell in enumerate(header_row):
            if not cell:
                continue
            cell_lower = str(cell).lower()
            
            for field, patterns in column_patterns.items():
                if any(pattern in cell_lower for pattern in patterns):
                    if field not in col_map:
                        col_map[field] = idx
        
        return col_map
    
    def _parse_holding_row(self, row: List, col_map: Dict[str, int], current_sector: str) -> Optional[Dict]:
        """Parse a single holding row"""
        try:
            # Get security name
            security_idx = col_map.get('security') or col_map.get('scrip', 0)
            security = str(row[security_idx]).strip() if security_idx < len(row) and row[security_idx] else None
            
            if not security or security.lower() in ['total', 'grand total', 'equity', 'net receivables', 'net receivables / (payables)']:
                return None
            
            # Clean security name
            security = re.sub(r'\s+', ' ', security)
            
            # Get percentage
            pct_idx = col_map.get('pct_of_aum')
            pct_of_aum = 0.0
            if pct_idx is not None and pct_idx < len(row):
                pct_of_aum = self.parse_percentage(row[pct_idx])
            
            # Get sector
            sector_idx = col_map.get('sector')
            sector = current_sector
            if sector_idx is not None and sector_idx < len(row) and row[sector_idx]:
                sector = str(row[sector_idx]).strip()
            
            # Get ISIN
            isin_idx = col_map.get('isin')
            isin = ""
            if isin_idx is not None and isin_idx < len(row) and row[isin_idx]:
                isin_value = str(row[isin_idx]).strip()
                # Validate ISIN format (typically INE + alphanumeric)
                if re.match(r'^IN[A-Z0-9]{10}$', isin_value):
                    isin = isin_value
            
            # Get market value
            mv_idx = col_map.get('market_value')
            market_value = None
            if mv_idx is not None and mv_idx < len(row):
                market_value = self.parse_number(row[mv_idx])
            
            # Get quantity
            qty_idx = col_map.get('quantity')
            quantity = None
            if qty_idx is not None and qty_idx < len(row):
                quantity = self.parse_number(row[qty_idx])
            
            return {
                'security': security,
                'isin': isin,
                'sector': sector,
                'pct_of_aum': pct_of_aum,
                'market_value': market_value,
                'quantity': quantity,
            }
            
        except Exception as e:
            logger.debug(f"Error parsing row: {e}")
            return None
    
    def _extract_holdings_from_text(self, text: str) -> List[Dict]:
        """Fallback: Extract holdings from raw text"""
        holdings = []
        
        # Pattern to match security name followed by percentage
        # e.g., "Zomato Ltd. 6.9"
        pattern = r'([A-Z][A-Za-z\s&\.\(\)]+(?:Ltd|Limited|Corp|Inc)?\.?)\s+(\d+\.\d+)'
        
        matches = re.findall(pattern, text)
        
        for match in matches:
            security = match[0].strip()
            pct = float(match[1])
            
            # Filter out noise
            if len(security) < 3 or pct > 100 or pct <= 0:
                continue
            
            # Skip common non-holding text
            skip_words = ['page', 'total', 'scheme', 'fund', 'year', 'month', 'return', 'nav']
            if any(word in security.lower() for word in skip_words):
                continue
            
            holdings.append({
                'security': security,
                'isin': '',
                'sector': 'Unknown',
                'pct_of_aum': pct,
                'market_value': None,
                'quantity': None,
            })
        
        return holdings
    
    def extract_sector_allocation(self, pdf_path: str) -> Dict[str, float]:
        """Extract sector allocation data from PDF"""
        sector_allocation = {}
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    
                    # Pattern for sector allocation
                    # e.g., "Banking 15.2%" or "Finance 12.5%"
                    pattern = r'([A-Za-z\s&\-]+)\s+(\d+\.?\d*)%'
                    matches = re.findall(pattern, text)
                    
                    for match in matches:
                        sector = match[0].strip()
                        pct = float(match[1])
                        
                        # Filter out non-sector entries
                        if len(sector) > 3 and pct > 0 and pct <= 100:
                            normalized_sector = self.normalize_sector(sector)
                            if normalized_sector not in sector_allocation:
                                sector_allocation[normalized_sector] = pct
                    
        except Exception as e:
            logger.error(f"Error extracting sector allocation: {e}")
        
        return sector_allocation
    
    def process_pdf(self, pdf_path: str, filename: str = None) -> Dict[str, Any]:
        """
        Main method to process a single PDF and extract all data.
        Returns structured data ready for analysis.
        """
        if filename is None:
            filename = os.path.basename(pdf_path)
        
        result = {
            'success': False,
            'filename': filename,
            'metadata': {},
            'holdings': [],
            'sector_allocation': {},
            'errors': []
        }
        
        try:
            # Extract date and AMC from filename
            date = self.extract_date_from_filename(filename)
            amc = self.extract_amc_from_filename(filename)
            
            # Extract portfolio table
            holdings, metadata = self.extract_portfolio_table(pdf_path)
            
            # Update metadata
            metadata['date'] = date
            metadata['amc'] = amc if amc != "Unknown" else metadata.get('amc', 'Unknown')
            
            # Extract sector allocation
            sector_allocation = self.extract_sector_allocation(pdf_path)
            
            # Enrich holdings with sector from allocation if available
            sector_keywords_map = self._build_sector_keyword_map(sector_allocation)
            
            # Get AUM for market value calculation
            aum_cr = metadata.get('aum_cr')
            
            for holding in holdings:
                if holding['sector'] == 'Unknown':
                    holding['sector'] = self._guess_sector(holding['security'], sector_keywords_map)
                else:
                    holding['sector'] = self.normalize_sector(holding['sector'])
                
                # Lookup ISIN if not already set
                if not holding.get('isin') or holding['isin'] == '':
                    holding['isin'] = self.isin_mapper.get_isin(holding['security'])
                
                # Calculate market value if AUM is available and market_value not already set
                if aum_cr and holding['pct_of_aum'] > 0:
                    if not holding.get('market_value'):
                        # Market Value = (Weight % / 100) * AUM
                        holding['market_value'] = round((holding['pct_of_aum'] / 100) * aum_cr, 2)
            
            # Build final records
            records = []
            for holding in holdings:
                record = HoldingRecord(
                    date=date,
                    amc=metadata['amc'],
                    fund_name=metadata['fund_name'],
                    security=holding['security'],
                    isin=holding.get('isin', ''),
                    sector=holding['sector'],
                    pct_of_aum=holding['pct_of_aum'],
                    market_value=holding.get('market_value'),
                    quantity=holding.get('quantity'),
                )
                records.append(asdict(record))
            
            result['success'] = True
            result['metadata'] = metadata
            result['holdings'] = records
            result['sector_allocation'] = sector_allocation
            
        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"Error processing {filename}: {e}")
        
        return result
    
    def _build_sector_keyword_map(self, sector_allocation: Dict[str, float]) -> Dict[str, List[str]]:
        """Build keyword map from sector names"""
        keyword_map = {}
        
        sector_keywords = {
            'Banking': ['bank', 'hdfc bank', 'icici bank', 'axis bank', 'kotak', 'sbi', 'indusind', 'federal bank'],
            'Information Technology': ['infosys', 'tcs', 'wipro', 'tech mahindra', 'hcl tech', 'coforge', 'ltimindtree', 'persistent', 'mphasis'],
            'Pharmaceuticals': ['pharma', 'dr.', 'cipla', 'sun pharma', 'lupin', 'divi', 'mankind', 'aurobindo', 'biocon'],
            'Financial Services': ['bajaj finance', 'bajaj finserv', 'cholamandalam', 'shriram', 'muthoot', 'manappuram', 'piramal', 'angel one'],
            'Automobile': ['auto', 'motor', 'maruti', 'tata motors', 'mahindra', 'hero motocorp', 'bajaj auto', 'tvs', 'ashok leyland', 'motherson', 'samvardhana'],
            'FMCG': ['hindustan unilever', 'itc', 'nestle', 'britannia', 'dabur', 'marico', 'godrej consumer', 'colgate', 'p&g'],
            'Telecom': ['bharti airtel', 'jio', 'vodafone', 'indus towers', 'telecom'],
            'Energy': ['reliance industries', 'ongc', 'oil india', 'bpcl', 'ioc', 'gail', 'ntpc', 'power grid', 'adani power', 'tata power'],
            'Metals & Mining': ['tata steel', 'jsw steel', 'hindalco', 'vedanta', 'coal india', 'nmdc', 'hindustan zinc'],
            'Real Estate': ['dlf', 'godrej properties', 'oberoi realty', 'prestige', 'phoenix mills', 'brigade', 'sobha', 'macrotech'],
            'Consumer Durables': ['titan', 'havells', 'voltas', 'whirlpool', 'crompton', 'dixon', 'amber', 'kalyan jewellers', 'v2 retail'],
            'Retailing': ['avenue supermarts', 'trent', 'dmart', 'zomato', 'swiggy', 'nykaa', 'tata consumer', 'devyani'],
            'Capital Markets': ['bse', 'mcx', 'angel one', 'icici securities', 'exchange', 'cams', 'kfin', 'religare'],
            'Aerospace & Defense': ['hal', 'hindustan aeronautics', 'bharat dynamics', 'bharat electronics', 'bel', 'zen technologies', 'paras defence', 'data patterns'],
            'Electrical Equipment': ['abb', 'siemens', 'cg power', 'ge vernova', 'suzlon', 'inox wind', 'premier energies', 'waaree', 'apar', 'polycab', 'kaynes'],
            'Industrial Manufacturing': ['larsen', 'l&t', 'thermax', 'cummins', 'grindwell norton', 'titagarh', 'ptc industries'],
            'Chemicals': ['asian paints', 'pidilite', 'upl', 'srf', 'gujarat fluorochemicals', 'navin fluorine', 'deepak nitrite'],
        }
        
        return sector_keywords
    
    def _guess_sector(self, security: str, keyword_map: Dict[str, List[str]]) -> str:
        """Guess sector based on security name"""
        security_lower = security.lower()
        
        for sector, keywords in keyword_map.items():
            if any(kw in security_lower for kw in keywords):
                return sector
        
        return "Other"
    
    def process_multiple_pdfs(self, pdf_files: List[Tuple[str, str]]) -> Dict[str, Any]:
        """
        Process multiple PDFs and combine results.
        pdf_files: List of tuples (file_path, filename)
        """
        all_holdings = []
        all_metadata = []
        all_sector_allocations = []
        errors = []
        
        for pdf_path, filename in pdf_files:
            result = self.process_pdf(pdf_path, filename)
            
            if result['success']:
                all_holdings.extend(result['holdings'])
                all_metadata.append({
                    'filename': filename,
                    **result['metadata']
                })
                all_sector_allocations.append({
                    'filename': filename,
                    'amc': result['metadata'].get('amc', 'Unknown'),
                    'fund_name': result['metadata'].get('fund_name', 'Unknown'),
                    'date': result['metadata'].get('date', ''),
                    'allocation': result['sector_allocation']
                })
            else:
                errors.extend(result['errors'])
        
        return {
            'holdings': all_holdings,
            'metadata': all_metadata,
            'sector_allocations': all_sector_allocations,
            'errors': errors
        }


def convert_to_dataframe(holdings: List[Dict]) -> pd.DataFrame:
    """Convert holdings list to pandas DataFrame"""
    df = pd.DataFrame(holdings)
    
    # Ensure proper column order
    columns = ['date', 'amc', 'fund_name', 'security', 'isin', 'sector', 'pct_of_aum', 'market_value', 'quantity']
    
    # Add missing columns
    for col in columns:
        if col not in df.columns:
            df[col] = None
    
    return df[columns]


def export_to_csv(holdings: List[Dict], output_path: str) -> str:
    """Export holdings to CSV file"""
    df = convert_to_dataframe(holdings)
    df.to_csv(output_path, index=False)
    return output_path


def export_to_json(holdings: List[Dict], output_path: str) -> str:
    """Export holdings to JSON file"""
    with open(output_path, 'w') as f:
        json.dump(holdings, f, indent=2, default=str)
    return output_path


# For direct testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        extractor = FactsheetExtractor()
        result = extractor.process_pdf(pdf_path)
        
        if result['success']:
            print(f"Successfully extracted {len(result['holdings'])} holdings")
            print(f"Fund: {result['metadata'].get('fund_name')}")
            print(f"AMC: {result['metadata'].get('amc')}")
            print(f"Date: {result['metadata'].get('date')}")
            
            # Preview first 5 holdings
            for holding in result['holdings'][:5]:
                print(f"  - {holding['security']}: {holding['pct_of_aum']}%")
        else:
            print(f"Extraction failed: {result['errors']}")
