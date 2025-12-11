"""
Mutual Fund Factsheet Extraction Web App
=========================================
Modern UI with Steel Blue (#4682B4) and White theme
"""

import gradio as gr
import pandas as pd
import os
import tempfile
import json
from datetime import datetime
from typing import List, Tuple, Optional

# Import backend
from backend import FactsheetExtractor, convert_to_dataframe, export_to_csv, export_to_json


class FactsheetApp:
    """Main application class for the Gradio interface"""
    
    def __init__(self):
        self.extractor = FactsheetExtractor()
        self.current_data = []
        self.output_dir = tempfile.mkdtemp()
        
    def process_files(self, files: List) -> Tuple[str, pd.DataFrame, str, str]:
        """Process uploaded PDF files and return results."""
        if not files:
            return "⚠️ No files uploaded. Please upload PDF files.", pd.DataFrame(), None, None
        
        self.current_data = []
        
        pdf_files = []
        for file in files:
            if file is not None:
                file_path = file.name if hasattr(file, 'name') else file
                filename = os.path.basename(file_path)
                pdf_files.append((file_path, filename))
        
        if not pdf_files:
            return "⚠️ No valid PDF files found.", pd.DataFrame(), None, None
        
        results = self.extractor.process_multiple_pdfs(pdf_files)
        self.current_data = results['holdings']
        
        num_files = len(pdf_files)
        num_holdings = len(results['holdings'])
        num_errors = len(results['errors'])
        
        status_parts = [f"✅ Successfully processed {num_files} file(s)"]
        status_parts.append(f"📊 Extracted {num_holdings} holdings")
        
        if num_errors > 0:
            status_parts.append(f"⚠️ {num_errors} error(s) encountered")
        
        if results['metadata']:
            status_parts.append("\n📁 Processed Files:")
            for meta in results['metadata']:
                fund_name = meta.get('fund_name', 'Unknown')
                amc = meta.get('amc', 'Unknown')
                date = meta.get('date', 'Unknown')
                aum = meta.get('aum_cr')
                aum_str = f" | AUM: ₹{aum:,.2f} Cr" if aum else ""
                status_parts.append(f"   • {amc} - {fund_name} ({date}){aum_str}")
        
        status_message = "\n".join(status_parts)
        
        if self.current_data:
            df = convert_to_dataframe(self.current_data)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(self.output_dir, f"holdings_{timestamp}.csv")
            json_path = os.path.join(self.output_dir, f"holdings_{timestamp}.json")
            
            export_to_csv(self.current_data, csv_path)
            export_to_json(self.current_data, json_path)
            
            return status_message, df, csv_path, json_path
        else:
            return status_message, pd.DataFrame(), None, None
    
    def get_analysis_summary(self) -> str:
        """Generate analysis summary of extracted data"""
        if not self.current_data:
            return "📭 No data available. Please process PDF files first."
        
        df = convert_to_dataframe(self.current_data)
        
        summary_parts = ["## 📈 Data Analysis Summary\n"]
        summary_parts.append(f"**Total Holdings:** {len(df)}")
        summary_parts.append(f"**Unique Securities:** {df['security'].nunique()}")
        summary_parts.append(f"**AMCs:** {', '.join(df['amc'].unique())}")
        summary_parts.append(f"**Funds:** {df['fund_name'].nunique()}")
        
        if 'market_value' in df.columns:
            total_mv = df['market_value'].sum()
            if total_mv > 0:
                summary_parts.append(f"**Total Market Value:** ₹{total_mv:,.2f} Cr")
        
        summary_parts.append("\n### 🏭 Top Sectors by Allocation")
        sector_avg = df.groupby('sector')['pct_of_aum'].sum().sort_values(ascending=False).head(8)
        for sector, pct in sector_avg.items():
            summary_parts.append(f"- **{sector}:** {pct:.2f}%")
        
        summary_parts.append("\n### 🏆 Top 10 Holdings")
        top_holdings = df.nlargest(10, 'pct_of_aum')[['security', 'pct_of_aum', 'market_value', 'sector']]
        for _, row in top_holdings.iterrows():
            mv_str = f" (₹{row['market_value']:,.2f} Cr)" if pd.notna(row['market_value']) else ""
            summary_parts.append(f"- **{row['security']}:** {row['pct_of_aum']:.2f}%{mv_str}")
        
        return "\n".join(summary_parts)
    
    def filter_data(self, amc_filter: str, sector_filter: str, min_weight: float) -> pd.DataFrame:
        """Filter the current data based on criteria"""
        if not self.current_data:
            return pd.DataFrame()
        
        df = convert_to_dataframe(self.current_data)
        
        if amc_filter and amc_filter != "All":
            df = df[df['amc'] == amc_filter]
        
        if sector_filter and sector_filter != "All":
            df = df[df['sector'] == sector_filter]
        
        if min_weight > 0:
            df = df[df['pct_of_aum'] >= min_weight]
        
        return df
    
    def get_filter_options(self) -> Tuple[List[str], List[str]]:
        """Get unique values for filter dropdowns"""
        if not self.current_data:
            return ["All"], ["All"]
        
        df = convert_to_dataframe(self.current_data)
        amcs = ["All"] + sorted(df['amc'].unique().tolist())
        sectors = ["All"] + sorted(df['sector'].unique().tolist())
        return amcs, sectors


def create_app():
    """Create Gradio app with Steel Blue theme"""
    
    app_instance = FactsheetApp()
    
    # Steel Blue Theme
    theme = gr.themes.Soft(
        primary_hue=gr.themes.colors.blue,
        secondary_hue=gr.themes.colors.slate,
        neutral_hue=gr.themes.colors.slate,
    ).set(
        body_background_fill="#f0f5fa",
        body_background_fill_dark="#1a1a2e",
        button_primary_background_fill="#4682B4",
        button_primary_background_fill_hover="#3a6d96",
        button_primary_text_color="white",
        button_secondary_background_fill="white",
        button_secondary_background_fill_hover="#e8f4fc",
        button_secondary_text_color="#4682B4",
        button_secondary_border_color="#4682B4",
        block_background_fill="white",
        block_border_color="#e0e7ef",
        block_label_background_fill="#4682B4",
        block_label_text_color="white",
        block_title_text_color="#4682B4",
        input_background_fill="white",
        input_border_color="#c5d5e5",
        input_border_color_focus="#4682B4",
        checkbox_background_color="#4682B4",
        checkbox_background_color_selected="#4682B4",
        slider_color="#4682B4",
    )
    
    with gr.Blocks(title="MF Factsheet Extractor") as app:
        
        # Custom Header with inline CSS
        gr.HTML("""
        <div style="
            background: linear-gradient(135deg, #4682B4 0%, #5a9fd4 100%);
            padding: 30px 20px;
            border-radius: 12px;
            margin-bottom: 20px;
            text-align: center;
            box-shadow: 0 4px 15px rgba(70, 130, 180, 0.3);
        ">
            <h1 style="
                color: white;
                margin: 0;
                font-size: 28px;
                font-weight: 700;
                text-shadow: 0 2px 4px rgba(0,0,0,0.1);
            ">📊 Mutual Fund Factsheet Extractor</h1>
            <p style="
                color: rgba(255,255,255,0.9);
                margin: 10px 0 0 0;
                font-size: 16px;
            ">Extract structured portfolio data from mutual fund factsheets</p>
        </div>
        """)
        
        with gr.Tabs():
            # Tab 1: Upload & Extract
            with gr.TabItem("📤 Upload & Extract"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 0 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            📁 Upload Files
                        </h3>
                        """)
                        
                        file_input = gr.File(
                            label="Drop PDF factsheets here",
                            file_count="multiple",
                            file_types=[".pdf"],
                            type="filepath"
                        )
                        
                        extract_btn = gr.Button(
                            "🚀 Extract Data",
                            variant="primary",
                            size="lg"
                        )
                        
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 20px 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            📋 Status
                        </h3>
                        """)
                        
                        status_output = gr.Textbox(
                            label="",
                            lines=8,
                            interactive=False,
                            show_label=False
                        )
                        
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 20px 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            📥 Download
                        </h3>
                        """)
                        
                        with gr.Row():
                            csv_download = gr.File(label="CSV", interactive=False)
                            json_download = gr.File(label="JSON", interactive=False)
                    
                    with gr.Column(scale=2):
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 0 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            📊 Extracted Holdings
                        </h3>
                        """)
                        
                        data_output = gr.Dataframe(
                            label="",
                            wrap=True,
                            show_label=False
                        )
            
            # Tab 2: Filter & Analyze
            with gr.TabItem("🔍 Filter & Analyze"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 0 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            🎛️ Filters
                        </h3>
                        """)
                        
                        amc_dropdown = gr.Dropdown(
                            label="AMC",
                            choices=["All"],
                            value="All",
                            interactive=True
                        )
                        
                        sector_dropdown = gr.Dropdown(
                            label="Sector",
                            choices=["All"],
                            value="All",
                            interactive=True
                        )
                        
                        weight_slider = gr.Slider(
                            label="Minimum Weight (%)",
                            minimum=0,
                            maximum=10,
                            value=0,
                            step=0.1
                        )
                        
                        with gr.Row():
                            filter_btn = gr.Button("🔍 Apply", variant="secondary")
                            refresh_btn = gr.Button("🔄 Refresh", variant="secondary")
                        
                        gr.HTML("<hr style='border-color: #4682B4; margin: 20px 0;'>")
                        
                        analyze_btn = gr.Button("📈 Generate Analysis", variant="primary")
                    
                    with gr.Column(scale=2):
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 0 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            📋 Filtered Results
                        </h3>
                        """)
                        
                        filtered_output = gr.Dataframe(
                            label="",
                            wrap=True,
                            show_label=False
                        )
                        
                        gr.HTML("""
                        <h3 style="color: #4682B4; margin: 20px 0 15px 0; padding-bottom: 10px; border-bottom: 2px solid #4682B4;">
                            📊 Analysis
                        </h3>
                        """)
                        
                        analysis_output = gr.Markdown()
            
            # Tab 3: Help
            with gr.TabItem("ℹ️ Help"):
                gr.HTML("""
                <div style="
                    background: black;
                    padding: 30px;
                    border-radius: 12px;
                    box-shadow: 0 2px 10px rgba(70, 130, 180, 0.1);
                ">
                    <h2 style="color: #4682B4; margin-top: 0;">📖 How to Use</h2>
                    
                    <div style="background: black; padding: 20px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #4682B4;">
                        <h3 style="color: #ffffff; margin-top: 0;">Step 1: Upload Files</h3>
                        <p>Drag & drop or click to upload mutual fund factsheet PDFs. Multiple files supported.</p>
                    </div>
                    
                    <div style="background: black; padding: 20px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #4682B4;">
                        <h3 style="color: #ffffff; margin-top: 0;">Step 2: Extract Data</h3>
                        <p style="color: #ffffff;>Click <strong>"Extract Data"</strong> to process files. The system will:</p>
                        <ul style="margin: 10px 0;">
                            <li style=color: #ffffff;">📄 Parse tables from PDFs</li>
                            <li>💰 Extract AUM & calculate market values</li>
                            <li>🔗 Match ISIN codes automatically</li>
                            <li>🏭 Classify sectors</li>
                        </ul>
                    </div>
                    
                    <div style="background: black; padding: 20px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #4682B4;">
                        <h3 style="color: #ffffff; margin-top: 0;">Step 3: Download & Analyze</h3>
                        <p>Download as CSV or JSON. Use Filter tab to explore data.</p>
                    </div>
                    
                    <hr style="border-color: #e0e7ef; margin: 25px 0;">
                    
                    <h2 style="color: #ffffff;">📋 Output Fields</h2>
                    <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
                        <tr style="background: #4682B4">
                            <th style="padding: 12px; text-align: left;">Field</th>
                            <th style="padding: 12px; text-align: left;">Description</th>
                        </tr>
                        <tr style="border-bottom: 1px solid #e0e7ef;">
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">date</code></td>
                            <td style="padding: 10px;">Factsheet date</td>
                        </tr>
                        <tr style="border-bottom: 1px solid background: #f8fbfd;">
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">amc</code></td>
                            <td style="padding: 10px;">Asset Management Company</td>
                        </tr>
                        <tr style="border-bottom: 1px solid background: #f8fbfd;">
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">security</code></td>
                            <td style="padding: 10px;">Stock name</td>
                        </tr>
                        <tr style="border-bottom: 1px solid background: #f8fbfd;">
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">isin</code></td>
                            <td style="padding: 10px;">ISIN code</td>
                        </tr>
                        <tr style="border-bottom: 1px solid background: #f8fbfd;">
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">sector</code></td>
                            <td style="padding: 10px;">Sector classification</td>
                        </tr>
                        <tr style="border-bottom: 1px solid background: #f8fbfd;">
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">pct_of_aum</code></td>
                            <td style="padding: 10px;">Portfolio weight (%)</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px;"><code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">market_value</code></td>
                            <td style="padding: 10px;">Holding value (₹ Crores)</td>
                        </tr>
                    </table>
                    
                    <hr style="border-color: #e0e7ef; margin: 25px 0;">
                    
                    <h2 style="color: #ffffff;">💡 Tips</h2>
                    <ul style="line-height: 1.8; style="color: #2F4F4F;">
                        <li>📝 File naming: <code style="background: #e8f4fc; padding: 2px 8px; border-radius: 4px; color: #4682B4;">AMC_FundName_YYYYMM.pdf</code></li>
                        <li>💰 Market Value = AUM × Weight %</li>
                        <li>🔗 1000+ ISINs auto-matched</li>
                    </ul>
                </div>
                """)
        
        # Footer
        gr.HTML("""
        <div style="
            text-align: center;
            padding: 20px;
            margin-top: 20px;
            color: #6b7280;
            font-size: 14px;
        ">
            <p style="margin: 0;">Built with ❤️ for Mutual Fund Analysis</p>
        </div>
        """)
        
        # Event handlers
        extract_btn.click(
            fn=app_instance.process_files,
            inputs=[file_input],
            outputs=[status_output, data_output, csv_download, json_download]
        )
        
        filter_btn.click(
            fn=app_instance.filter_data,
            inputs=[amc_dropdown, sector_dropdown, weight_slider],
            outputs=[filtered_output]
        )
        
        refresh_btn.click(
            fn=app_instance.get_filter_options,
            outputs=[amc_dropdown, sector_dropdown]
        )
        
        analyze_btn.click(
            fn=app_instance.get_analysis_summary,
            outputs=[analysis_output]
        )
    
    return app


if __name__ == "__main__":
    app = create_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False
    )
