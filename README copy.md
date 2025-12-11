# Mutual Fund Factsheet Data Extractor

An automated pipeline for extracting structured portfolio holdings data from mutual fund factsheet PDFs.

## 📋 Overview

This tool extracts and normalizes data from mutual fund factsheets into analysis-ready formats (CSV/JSON). It supports:

- **Multi-page table extraction** - Handles holdings spanning multiple pages
- **Multi-document processing** - Batch process multiple factsheets at once
- **Field normalization** - Consistent sector and AMC naming across different fund houses
- **Export formats** - CSV for spreadsheets, JSON for programmatic access

## 🚀 Quick Start

### Installation

```bash
# Install dependencies
pip install pdfplumber pandas gradio

# Or using requirements.txt
pip install -r requirements.txt
```

### Running the Application

```bash
# Start the web interface
python app.py
```

The application will be available at `http://localhost:7860`

### Command Line Usage

```bash
# Process a single PDF
python backend.py path/to/factsheet.pdf
```

## 📁 Project Structure

```
mutual_fund_extractor/
├── backend.py       # Core extraction logic
├── app.py           # Gradio web interface
├── data/            # Sample and ISIN list files
├── requirements.txt # Python dependencies
└── README.md       
```

## 📊 Output Schema

The extracted data follows this schema:

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Factsheet date (YYYY-MM-DD) |
| `amc` | string | Asset Management Company |
| `fund_name` | string | Full fund name |
| `security` | string | Stock/Security name |
| `isin` | string | ISIN code (if available) |
| `sector` | string | Normalized sector classification |
| `pct_of_aum` | float | Portfolio weight (% of AUM) |
| `market_value` | float | Market value in INR (if available) |
| `quantity` | float | Number of shares/units (if available) |

## 📝 File Naming Convention

For best results, use this naming convention for your PDF files:

```
AMC_FundName_YYYYMM.pdf
```

Examples:
- `motilaloswal_largeandmidcap_202412.pdf`
- `hdfc_largeandmidcap_202411.pdf`
- `icici_largenmidcap_202410.pdf`

## 🔧 Configuration

### Sector Normalization

The tool normalizes sector names across AMCs. Common mappings include:

| Input Variations | Normalized Name |
|-----------------|-----------------|
| IT - Software, IT-Software, Software | Information Technology |
| Banks, Banking, Private Sector Bank | Banking |
| Pharmaceuticals & Biotechnology, Pharma | Pharmaceuticals |

Custom mappings can be added in `backend.py`:

```python
SECTOR_MAPPINGS = {
    'custom sector name': 'Normalized Name',
    ...
}
```

### AMC Normalization

Similarly, AMC names are normalized:

| Input | Normalized |
|-------|------------|
| motilal oswal | Motilal Oswal |
| icici prudential | ICICI Prudential |

## 🛠️ Technical Details

### PDF Extraction Pipeline

1. **Table Detection** - Uses pdfplumber to identify and extract tables
2. **Column Mapping** - Automatically maps columns based on header keywords
3. **Multi-page Handling** - Merges tables spanning multiple pages
4. **Text Fallback** - Falls back to regex extraction if tables aren't detected
5. **Normalization** - Standardizes sectors, AMCs, and numeric formats

### Supported PDF Formats

- Standard text-based PDFs ✅
- Tables with merged cells ✅
- Multi-column layouts ✅
- Scanned PDFs (requires OCR) ⚠️ Limited support

## 📈 Example Usage

### Web Interface

1. Open `http://localhost:7860`
2. Upload one or more PDF factsheets
3. Click "Extract Data"
4. View results in the table
5. Download as CSV or JSON
6. Use Filter tab for exploration

### Programmatic Usage

```python
from backend import FactsheetExtractor, export_to_csv

# Initialize extractor
extractor = FactsheetExtractor()

# Process single PDF
result = extractor.process_pdf('factsheet.pdf')

if result['success']:
    holdings = result['holdings']
    print(f"Extracted {len(holdings)} holdings")
    
    # Export
    export_to_csv(holdings, 'output.csv')

# Process multiple PDFs
pdf_files = [
    ('file1.pdf', 'amc1_fund_202412.pdf'),
    ('file2.pdf', 'amc2_fund_202411.pdf'),
]

results = extractor.process_multiple_pdfs(pdf_files)
all_holdings = results['holdings']
```

## ⚠️ Limitations

- Scanned/image-based PDFs may not extract accurately
- Very complex layouts may require manual verification
- ISIN extraction depends on PDF content availability
- Some older factsheet formats may need custom handling


## Acknowledgments

Built using:
- [pdfplumber](https://github.com/jsvine/pdfplumber) - PDF parsing
- [pandas](https://pandas.pydata.org/) - Data processing
- [Gradio](https://gradio.app/) - Web interface
