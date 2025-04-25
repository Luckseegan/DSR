import streamlit as st
import pandas as pd
from datetime import timedelta
from rapidfuzz import process, fuzz
import time
import plotly.express as px



# ========================
# CONFIGURATION MANAGER
# ========================
import json
from pathlib import Path
import streamlit as st
import pandas as pd

class ConfigurationManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigurationManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        self.config_path = Path("config.json")
        self.default_config = {
            "global": {
                "date_range_weeks": 2,
                "fuzzy_threshold": 45,
                "default_bond_type": "FCL"
            },
            "columns": [
                "HBL", "ETA", "SBU", "CARGO READY DATE", "Origin", "Port", 
                "Shipper", "Inv #", "PO #", "Description of Goods", "No of Cartons",
                "Type", "Gross Weight", "Actual CBM", "LCL,FCL Status",
                "Origin Vessel", "Connecting vessel", "Voyage No", "ATD",
                "ATA", "ETB", "Estimated Clearance", "Status", "HB/L NO",
                "Container No.", "Bond or Non Bond", "Cleared By", "REMARK",
                "Delivery date", "Delivery location","CUSDEC No","CUSDEC Date", "sheet"
            ],
            "mappings":{
                    "expo": {
                        "bond": {
                                "HBL": "HBL", "PO #": "PO # ", "Gross Weight": "Gross Weight",
                                "Status": "Status", "Voyage No": 29, "ATD": "ATD",
                                "Description of Goods": "Description of Goods", "ETA": "ETA",
                                "No of Cartons": "No of Cartons", "ATA": "ATA",
                                "CARGO READY DATE": "Shipment Ready Date , Invoice Date",
                                "Origin": "Port of Origin", "Port": "Port of Origin",
                                "Inv #": "SUPPLIER Invoice", "Actual CBM": "CBM (Last two decimals)",
                                "LCL,FCL Status": "LCL,FCL", "Origin Vessel": 27,
                                "Connecting vessel": 28,
                                "Bond or Non Bond": "BOND/Non BOND", "REMARK": "Comments",
                                "Delivery date": "Delivery Date", "Delivery location": "Location",
                                "Container No.": "Container No", "Shipper": "Supplier( Name as for the Invoice)",
                                "Type": " Type (In two letters)", "SBU": "Consignee","CUSDEC No":48,"CUSDEC Date":"Cusdec Date"
                            },
                        "non_bond": {
                            "HBL": "HBL",
                            "PO #": 3,
                            "Gross Weight": "Gross Weight",
                            "Status": "Status",
                            "Voyage No": "Voyage No",
                            "ATD": "LATEST ATD",
                            "Description of Goods": "Description of Goods",
                            "ETA": "ETA",
                            "No of Cartons": "No of Cartons",
                            "ATA": "LATEST ATA",
                            "CARGO READY DATE": "Shipment Ready Date , Invoice Date",
                            "Origin": "Port of Origin",
                            "Port": "Port of Origin",
                            "Inv #": "SUPPLIER Invoice",
                            "Actual CBM": 18,
                            "LCL,FCL Status": "LCL , FCL",
                            "Origin Vessel": 25,
                            "Connecting vessel": "Second Vessel , Flight",
                            "Bond or Non Bond": "BOND/Non BOND",
                            "REMARK": "Comments",
                            "Delivery date": 51,
                            "Delivery location": 53,
                            "Container No.": "Container No",
                            "Shipper": "Supplier( Name as for the Invoice)",
                            "Type": "TYPE (IN TWO LETTERS)",
                            "SBU": "Consignee",
                            "CUSDEC No":50,
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
                        },
                        "fcl": {
                                "HBL": "HBL", "PO #": 3, "Gross Weight": "Gross Weight",
                                "Status": "Status", "Voyage No": "Voyage No", "ATD": "LATEST ATD",
                                "Description of Goods": "Description of Goods", "ETA": "ETA",
                                "No of Cartons": "No of Cartons", "ATA": "LATEST ATA",
                                "CARGO READY DATE": "Shipment Ready Date , Invoice Date",
                                "Origin": "Port of Origin", "Port": "Port of Origin",
                                "Inv #": "SUPPLIER Invoice", "Actual CBM": 18,
                                "LCL,FCL Status": "LCL , FCL", "Origin Vessel": 23,
                                "Connecting vessel": "Second Vessel , Flight",
                                "Bond or Non Bond": "BOND/Non BOND", "REMARK": "Comments",
                                "Delivery date": 52, "Delivery location": 54,
                                "Container No.": "Container No", "Shipper": "Supplier( Name as for the Invoice)",
                                "Type": 14, "SBU": "Consignee",
                            "CUSDEC No":"Cusdec No.",
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
                            }
                    },
                    "maersk": {
                        "dsr": {
        # Direct column name mappings
        "ATA": "ATA",
        "ATD": "ATD",
        "Description of Goods": "Description of Goods",
        "ETA": "ETA",
        "Gross Weight": "Gross Weight",
        "HBL": "HBL",
        "No of Cartons": "No of Cartons",
        "Status": "Status",
        "Voyage No": "Voyage No",
        "Actual CBM": "CBM (Last two decimals)",
        "Container No.": "Container No",
        "LCL,FCL Status": "LCL , FCL",
        "Origin": "Port of Origin",
        "Port": "Port of Origin",
        "Shipper": "Supplier( Name as for the Invoice)",
        "REMARK": "PENDING TASKS",
        "Delivery date": "Delivery Date",
        "Delivery location": "Delivery Location",
        "Inv #": 3,
        "PO #": 4,
        "CARGO READY DATE": 10,
        "Origin Vessel": 24,
        "Connecting vessel": 25,
        "Type": 15,
        "SBU": "Consignee",
                            "CUSDEC No":"Cusdec No.",
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
    },
                        "archived": {
        # Direct column name mappings
        "ATA": "ATA",
        "ATD": "ATD",
        "Description of Goods": "Description of Goods",
        "ETA": "ETA",
        "Gross Weight": "Gross Weight",
        "HBL": "HBL",
        "No of Cartons": "No of Cartons",
        "Status": "Status",
        "Voyage No": "Voyage No",
        "Actual CBM": "CBM (Last two decimals)",
        "Container No.": "Container No",
        "LCL,FCL Status": "LCL , FCL",
        "Origin": "Port of Origin",
        "Port": "Port of Origin",
        "Shipper": "Supplier( Name as for the Invoice)",
        "REMARK": "PENDING TASKS",
        "Delivery date": "Delivery Date",
        "Delivery location": "Delivery Location",
        "Inv #": 3,
        "PO #": 4,
        "CARGO READY DATE": 10,
        "Origin Vessel": 24,
        "Connecting vessel": 25,
        "Type": 15,
        "SBU": "Consignee",
                            "CUSDEC No":"Cusdec No.",
                            "CUSDEC Date":"CUSDEC SUBMITTED DATE"
    }
                    },
                    "globe": {
                        "ongoing": {
                                    # Exact matches (auto-aligned columns)
                                    "ETA": "ETA DATE",
                                    "PO #": "PO #",
                                    # Manual mappings for missing columns
                                    "ATA":  None,  # Assuming MAS REF # represents ATA (adjust if needed)
                                    "ATD":  None,  # Assuming VESSEL/VOYAGE can correspond to ATD (adjust if needed)
                                    "Actual CBM": "CBM",
                                    "Bond or Non Bond": "FCL",  # Assuming default bond type as FCL
                                    "CARGO READY DATE": "CLEARED DATE",  # Assuming "CLEARED DATE" corresponds to CARGO READY DATE
                                    "Cleared By": None,  # If not available, fill with NaN
                                    "Connecting vessel": None,  # Assuming VESSEL/VOYAGE corresponds to Connecting vessel
                                    "Container No.": "CONTAINER #",
                                    "Delivery date":  None,  # Using ETA as proxy for Delivery Date
                                    "Delivery location": None,  # Assuming Delivery Location is missing
                                    "Description of Goods": "CARGO DESCRIPTION",
                                    "ETB": None,  # Assuming ETB is missing
                                    "Estimated Clearance": None,  # Assuming this is missing
                                    "Gross Weight": "G/W (KG)",
                                    "HB/L NO": "BL #",  # Assuming BL # corresponds to HB/L NO
                                    "HBL": "BL #",  # Assuming BL # corresponds to HBL
                                    "Inv #": None,  # Assuming Inv # is missing
                                    "LCL,FCL Status": None,  # Assuming this is missing
                                    "No of Cartons": None,  # Assuming this is missing
                                    "Origin": None,  # Assuming Origin is missing
                                    "Origin Vessel": "VESSEL/VOYAGE",  # Assuming Origin Vessel is missing
                                    "Port": None,  # Assuming Port is missing
                                    "REMARK": "REMARKS",
                                    "SBU": 2,  # Assuming SBU is missing
                                    "Shipper": "SHIPPER",
                                    "Status": "STATUS",
                                    "Type": None,  # Assuming Type is missing
                                    "Voyage No": None,
                            "CUSDEC No":"ENTRY # / DATE",
                                },
                        "cleared":  {
                                        # Exact matches (auto-aligned columns)
                                        "ETA": "ETA DATE",
                                        "PO #": "PO #",
                                        
                                        # Manual mappings for missing columns
                                        "ATA":  None,  # Assuming MAS REF # represents ATA (adjust if needed)
                                        "ATD":  None,  # Assuming VESSEL/VOYAGE can correspond to ATD (adjust if needed)
                                        "Actual CBM": "CBM",
                                        "Bond or Non Bond": "FCL",  # Assuming default bond type as FCL
                                        "CARGO READY DATE": "CLEARED DATE",  # Assuming "CLEARED DATE" corresponds to CARGO READY DATE
                                        "Cleared By": None,  # If not available, fill with NaN
                                        "Connecting vessel": None,  # Assuming VESSEL/VOYAGE corresponds to Connecting vessel
                                        "Container No.": "CONTAINER #",
                                        "Delivery date":  None,  # Using ETA as proxy for Delivery Date
                                        "Delivery location": None,  # Assuming Delivery Location is missing
                                        "Description of Goods": "CARGO DESCRIPTION",
                                        "ETB": None,  # Assuming ETB is missing
                                        "Estimated Clearance": None,  # Assuming this is missing
                                        "Gross Weight": "G/W (KG)",
                                        "HB/L NO": "BL NO",  # Assuming BL # corresponds to HB/L NO
                                        "HBL": "BL NO",  # Assuming BL # corresponds to HBL
                                        "Inv #": None,  # Assuming Inv # is missing
                                        "LCL,FCL Status": None,  # Assuming this is missing
                                        "No of Cartons": None,  # Assuming this is missing
                                        "Origin": None,  # Assuming Origin is missing
                                        "Origin Vessel": "VESSEL/VOYAGE",  # Assuming Origin Vessel is missing
                                        "Port": None,  # Assuming Port is missing
                                        "REMARK": "REMARKS",
                                        "SBU": 2,  # Assuming SBU is missing
                                        "Shipper": "SHIPPER",
                                        "Status": "STATUS",
                                        "Type": None,  # Assuming Type is missing
                                        "Voyage No": None,
                                        "CUSDEC No":"ENTRY #",
  # Assuming Voyage No is missing
                                    }

                    },
                    "scanwell": {
                        "unichela": {
                                "ETA": "ETA", "ATD": "ATD", "Bond or Non Bond": "Bond or Non Bond",
                                "Gross Weight": "Gross Weight", "No of Cartons": "No of Cartons", 
                                "Voyage No": "Voyage No", "ATA": " ATA", "Actual CBM": "CBM",
                                "CARGO READY DATE": "Shipment Ready Date/Invoice Date", "Cleared By": None,
                                "Connecting vessel": "Second Vessel", "Container No.": "Container No",
                                "Delivery date": "Delivery Date", "Delivery location": None,
                                "Description of Goods": "Discriptin of Goods", "ETB": None,
                                "Estimated Clearance": "Planned Clearance", "HB/L NO": "HBL NO",
                                "HBL": "HBL NO", "Inv #": "IA1:AC1NVOICE", "LCL,FCL Status": "LCL/FCL",
                                "Origin": "Port of Origin", "Origin Vessel": "First Vessel", "PO #": 1,
                                "Port": "Port of Origin", "REMARK": "Comments", "SBU": 3,
                                "Shipper": "Supplier", "Status": "Pre Alert Status", "Type": "CTN Type",
                                "CUSDEC No":"Cusdec No",
                            },
                        "bodyline": {
                            "ETA": "ETA", "ATD": "ATD", "Bond or Non Bond": "Bond or Non Bond",
                            "Gross Weight": "Gross Weight", "No of Cartons": "No of Cartons", 
                            "Voyage No": "Voyage No", "ATA": " ATA", "Actual CBM": "CBM",
                            "CARGO READY DATE": "Shipment Ready Date/Invoice Date", "Cleared By": None,
                            "Connecting vessel": "Second Vessel", "Container No.": "Container No",
                            "Delivery date": "Delivery Date", "Delivery location": None,
                            "Description of Goods": "Discriptin of Goods", "ETB": None,
                            "Estimated Clearance": "Planned Clearance", "HB/L NO": "HBL NO",
                            "HBL": "HBL NO", "Inv #": "INVOICE", "LCL,FCL Status": "LCL/FCL",
                            "Origin": "Port of Origin", "Origin Vessel": "First Vessel", "PO #": 1,
                            "Port": "Port of Origin", "REMARK": "Remarks", "SBU": 3,
                            "Shipper": "Supplier", "Status": "Pre Alert Status", "Type": "CTN Type",
                            "CUSDEC No":"Cusdec No",

                        }
                    }
                }
                ,
            "target_consignees": ["Unichela", "MAS Capital", "Bodyline"]
        }
        self.config = self._load_config()
        
    def _load_config(self):
        try:
            if self.config_path.exists():
                with open(self.config_path, "r") as f:
                    return json.load(f)
            return self.default_config
        except Exception as e:
            st.error(f"Error loading config: {str(e)}")
            return self.default_config
        
    def save_config(self):
        try:
            # Convert string numbers back to integers before saving
            def convert_numbers(obj):
                if isinstance(obj, dict):
                    return {k: convert_numbers(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numbers(item) for item in obj]
                elif isinstance(obj, str) and obj.isdigit():
                    return int(obj)
                return obj
                
            with open(self.config_path, "w") as f:
                json.dump(convert_numbers(self.config), f, indent=4)
            return True
        except Exception as e:
            st.error(f"Error saving config: {str(e)}")
            return False

    # def save_config(self):
    #     try:
    #         with open(self.config_path, "w") as f:
    #             json.dump(self.config, f, indent=4)
    #         return True
    #     except Exception as e:
    #         st.error(f"Error saving config: {str(e)}")
    #         return False
            
    def get_all_columns(self):
        """Combine required and optional columns"""
        return self.config["columns"]
        # return self.config["columns"]["required"] + self.config["columns"]["optional"]
        
    def get_mappings(self, file_type, sheet_type):
        """Get mappings for specific file and sheet type"""
        try:
            return self.config["mappings"][file_type][sheet_type]
        except KeyError:
            return {}
            
    def update_mappings(self, file_type, sheet_type, new_mappings):
        """Update mappings for a specific sheet"""
        if file_type not in self.config["mappings"]:
            self.config["mappings"][file_type] = {}
        self.config["mappings"][file_type][sheet_type] = new_mappings
        
    def add_column(self, column_name, is_required=False):
        """Add a new column to configuration"""
        target_list = "required" if is_required else "optional"
        if column_name not in self.config["columns"][target_list]:
            self.config["columns"][target_list].append(column_name)
            return True
        return False
        
    def remove_column(self, column_name):
        """Remove a column from configuration"""
        # Don't allow removal of required columns
        if column_name in self.config["columns"]["required"]:
            return False
            
        if column_name in self.config["columns"]["optional"]:
            self.config["columns"]["optional"].remove(column_name)
            
            # Clean up any mappings using this column
            for file_type in self.config["mappings"]:
                for sheet_type in self.config["mappings"][file_type]:
                    if column_name in self.config["mappings"][file_type][sheet_type]:
                        del self.config["mappings"][file_type][sheet_type][column_name]
            return True
        return False

    def validate_config(self):
        errors = []
        # Check mappings reference valid columns
        for file_type, sheets in self.config.get("mappings", {}).items():
            for sheet_type, mappings in sheets.items():
                for target_col in mappings.keys():
                    if target_col not in self.get_all_columns():
                        errors.append(f"Mapping references non-existent column: {target_col} in {file_type}/{sheet_type}")
        return errors



# Initialize configuration manager
config_manager = ConfigurationManager()

# ========================
# CONFIGURATION UI
# ========================
def show_configuration_ui():
    st.sidebar.header("Configuration")
    
    with st.sidebar.expander("⚙️ Settings", expanded=False):
        tab1, tab2, tab3 = st.tabs(["Columns", "Mappings", "Global"])
        
        with tab1:
            show_column_management()
            
        with tab2:
            show_mapping_management()
            
        with tab3:
            show_global_settings()

def show_column_management():
    st.subheader("Column Management")
    
    # Single list of all columns
    st.write("**All Columns**")
    columns_df = st.data_editor(
        pd.DataFrame(config_manager.config["columns"], columns=["Column"]),
        num_rows="dynamic",
        key="columns_editor",
        hide_index=True
    )
    
    # Add new column
    st.subheader("Add New Column")
    new_col = st.text_input("Column Name", key="new_column_name")
    
    if st.button("Add Column", key="add_column_btn"):
        if not new_col.strip():
            st.error("Column name cannot be empty")
        elif new_col.strip() not in config_manager.config["columns"]:
            config_manager.config["columns"].append(new_col.strip())
            if config_manager.save_config():
                st.success(f"Added column: {new_col}")
                st.rerun()
        else:
            st.error(f"Column '{new_col}' already exists")
    
    # Remove column
    st.subheader("Remove Column")
    col_to_remove = st.selectbox(
        "Select column to remove",
        [""] + config_manager.config["columns"],
        key="col_to_remove"
    )
    
    if col_to_remove and st.button("Remove Column", key="remove_column_btn"):
        if col_to_remove in config_manager.config["columns"]:
            config_manager.config["columns"].remove(col_to_remove)
            # Clean up any mappings using this column
            for file_type in config_manager.config["mappings"]:
                for sheet_type in config_manager.config["mappings"][file_type]:
                    if col_to_remove in config_manager.config["mappings"][file_type][sheet_type]:
                        del config_manager.config["mappings"][file_type][sheet_type][col_to_remove]
            if config_manager.save_config():
                st.success(f"Removed column: {col_to_remove}")
                st.rerun()


def show_mapping_management():
    st.subheader("Mapping Management")
    
    file_type = st.selectbox(
        "Select File Type",
        ["expo", "maersk", "globe", "scanwell"],
        key="mapping_file_type"
    )
    
    sheet_type = st.selectbox(
        "Select Sheet Type",
        list(config_manager.config["mappings"].get(file_type, {}).keys()),
        key="mapping_sheet_type"
    )
    
    if not sheet_type:
        st.warning(f"No sheets defined for {file_type}")
        return
    
    current_mappings = config_manager.get_mappings(file_type, sheet_type)
    updated_mappings = {}
    
    for target_col in config_manager.get_all_columns():
        current_val = current_mappings.get(target_col, "")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            st.markdown(f"**{target_col}**")
        
        with col2:
            # Use text input but validate for numbers
            input_val = st.text_input(
                "Source column or index",
                value=str(current_val) if current_val is not None else "",
                key=f"mapping_{file_type}_{sheet_type}_{target_col}",
                label_visibility="collapsed"
            )
            
            # Convert to int if it's a number, otherwise keep as string
            updated_mappings[target_col] = int(input_val) if input_val.isdigit() else input_val
    
    if st.button("Save Mappings", key="save_mappings_btn"):
        # Filter out empty mappings but keep 0 as valid index
        config_manager.update_mappings(
            file_type,
            sheet_type,
            {k: v for k, v in updated_mappings.items() if v != ""}
        )
        if config_manager.save_config():
            st.success("Mappings saved!")
        else:
            st.error("Failed to save mappings")

def show_global_settings():
    st.subheader("Global Settings")
    
    # Date range weeks
    weeks = st.number_input(
        "Date Range Weeks",
        min_value=1,
        max_value=8,
        value=config_manager.config["global"]["date_range_weeks"],
        key="date_range_weeks_cfg"
    )
    
    # Fuzzy threshold
    threshold = st.slider(
        "Fuzzy Matching Threshold",
        min_value=0,
        max_value=100,
        value=config_manager.config["global"]["fuzzy_threshold"],
        key="fuzzy_threshold_cfg"
    )
    
    # Default bond type
    bond_type = st.selectbox(
        "Default Bond Type",
        ["FCL", "LCL", "Non Bond"],
        index=["FCL", "LCL", "Non Bond"].index(config_manager.config["global"]["default_bond_type"]),
        key="default_bond_type_cfg"
    )
    
    # Target consignees
    st.subheader("Target Consignees")
    consignees = st.text_area(
        "Consignees (one per line)",
        value="\n".join(config_manager.config["target_consignees"]),
        key="target_consignees_cfg"
    )
    
    if st.button("Save Global Settings", key="save_global_btn"):
        config_manager.config["global"].update({
            "date_range_weeks": weeks,
            "fuzzy_threshold": threshold,
            "default_bond_type": bond_type
        })
        config_manager.config["target_consignees"] = [
            c.strip() for c in consignees.split("\n") if c.strip()
        ]
        
        errors = config_manager.validate_config()
        if errors:
            for error in errors:
                st.error(error)
        elif config_manager.save_config():
            st.success("Global settings saved!")
        else:
            st.error("Failed to save global settings")





def init_session_state():
    session_defaults = {
        'current_step': 1,  # 1=Expo, 2=Maersk, 3=Type3, 4=Type4, 5=Final
        'expo_data': pd.DataFrame(),
        'maersk_data': pd.DataFrame(),  # New Maersk data storage
        'globe_data': pd.DataFrame(),
        'scanwell_data': pd.DataFrame(),
        'scanwell_processed': False,        'expo_processed': False,
        'maersk_processed': False,  # New Maersk status flag
        'globe_processed': False,
        'current_file': None,
        'selected_reference_date': pd.Timestamp.today().normalize(),  # NEW
        'config_manager': config_manager,  # Add this line
        # Optional: Track individual Maersk sheets if needed
        'maersk_dsr_processed': False,
        'maersk_archived_processed': False,
        'scanwell_unichela_processed': False,
        'scanwell_bodyline_processed': False
    }
    
    for key, value in session_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()


# ========================
# COMMON PROCESSING LOGIC
# ========================


# Initialize ConfigurationManager at the top (after imports)
config_manager = ConfigurationManager()

# Create helper functions to access configuration
def get_target_columns():
    """Get combined list of required and optional columns"""
    return config_manager.get_all_columns()

def get_target_consignees():
    """Get current target consignees"""
    return config_manager.config["target_consignees"]

def get_fuzzy_threshold():
    """Get current fuzzy matching threshold"""
    return config_manager.config["global"]["fuzzy_threshold"]

def get_date_range_weeks():
    """Get current date range setting"""
    return config_manager.config["global"]["date_range_weeks"]

# def filter_and_match_consignee(df, target_consignees=None, date_column="ETA", threshold=None):
    try:
        # Use configured values if not provided
        if target_consignees is None:
            target_consignees = get_target_consignees()
        if threshold is None:
            threshold = get_fuzzy_threshold()
            
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        # Use configured date range
        ref_date = st.session_state.selected_reference_date
        weeks = get_date_range_weeks()
        start_date = ref_date - timedelta(weeks=weeks)
        end_date = ref_date + timedelta(weeks=weeks)

        # Rest of the function remains the same...
        # ... [keep all existing code]
        # Filter rows based on ETA date range
        filtered_by_eta = df[
            (df[date_column] >= start_date) & (df[date_column] <= end_date)
        ].copy()

        # Check if there are any rows after filtering
        if filtered_by_eta.empty:
            return pd.DataFrame(), pd.DataFrame()  # Return empty DataFrames if no rows match the date filter

        # Normalize the consignee names for matching
        def normalize_text(text):
            return ''.join(e for e in str(text).lower() if e.isalnum()) if pd.notna(text) else ""

        filtered_by_eta["Consignee_clean"] = filtered_by_eta["Consignee"].apply(normalize_text)
        normalized_targets = [normalize_text(name) for name in target_consignees]

        # Function to get the best match using fuzzy matching
        def get_best_match_score(text):
            if not text:
                return pd.Series([pd.NA, 0])
            match_data = process.extractOne(text, normalized_targets, scorer=fuzz.token_set_ratio)
            if not match_data:
                return pd.Series([pd.NA, 0])
            return pd.Series([match_data[0], match_data[1]])

        filtered_by_eta[["BestMatch", "Score"]] = filtered_by_eta["Consignee_clean"].apply(get_best_match_score)

        # Filter rows based on matching score
        matched_df = filtered_by_eta[filtered_by_eta["Score"] >= threshold].copy()
        removed_df = filtered_by_eta[filtered_by_eta["Score"] < threshold].copy()

        # Check if matched_df is empty and handle accordingly
        if matched_df.empty:
            return pd.DataFrame(), removed_df  # Return empty matched_df and removed_df with the non-matching rows

        return matched_df, removed_df

    except Exception as e:
        print(f"❌ Error in filter_and_match_consignee: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def filter_and_match_consignee(df, target_consignees=None, date_column="ETA", threshold=None):
    try:
        # Use configured values if not provided
        if target_consignees is None:
            target_consignees = get_target_consignees()
        if threshold is None:
            threshold = get_fuzzy_threshold()
            
        # Only process date if filtering is enabled and column exists
        if st.session_state.get('use_date_filter', True) and date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            
            # Use configured date range
            ref_date = st.session_state.selected_reference_date
            weeks = get_date_range_weeks()
            start_date = ref_date - timedelta(weeks=weeks)
            end_date = ref_date + timedelta(weeks=weeks)

            # Filter rows based on ETA date range
            filtered_df = df[
                (df[date_column] >= start_date) & (df[date_column] <= end_date)
            ].copy()
        else:
            filtered_df = df.copy()

        # Rest of the function remains the same...
        # Normalize the consignee names for matching
        def normalize_text(text):
            return ''.join(e for e in str(text).lower() if e.isalnum()) if pd.notna(text) else ""

        filtered_df["Consignee_clean"] = filtered_df["Consignee"].apply(normalize_text)
        normalized_targets = [normalize_text(name) for name in target_consignees]

        # Function to get the best match using fuzzy matching
        def get_best_match_score(text):
            if not text:
                return pd.Series([pd.NA, 0])
            match_data = process.extractOne(text, normalized_targets, scorer=fuzz.token_set_ratio)
            if not match_data:
                return pd.Series([pd.NA, 0])
            return pd.Series([match_data[0], match_data[1]])

        filtered_df[["BestMatch", "Score"]] = filtered_df["Consignee_clean"].apply(get_best_match_score)

        # Filter rows based on matching score
        matched_df = filtered_df[filtered_df["Score"] >= threshold].copy()
        removed_df = filtered_df[filtered_df["Score"] < threshold].copy()

        return matched_df, removed_df

    except Exception as e:
        print(f"❌ Error in filter_and_match_consignee: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def process_consignee_matching(df, target_consignees=None, date_column="ETA", consignee_column="Consignee", threshold=None):
    """Processes consignee name matching using fuzzy matching"""
    # Use configured values if not provided
    if target_consignees is None:
        target_consignees = get_target_consignees()
    if threshold is None:
        threshold = get_fuzzy_threshold()
        
    # Rest of the function remains the same...
    # ... [keep all existing code]

    # Step 1: Convert ETA to datetime
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    # Step 2: Add ETA Remark for missing values
    df["ETA Remark"] = df[date_column].apply(lambda x: "NA" if pd.isna(x) else "")

    # Step 3: Rename to standard column names for processing
    df = df.rename(columns={consignee_column: "Consignee", date_column: "ETA"})

    # Step 4: Apply filtering and fuzzy matching
    matched_df, removed_df = filter_and_match_consignee(df, target_consignees, date_column="ETA", threshold=threshold)


    return matched_df, removed_df

def map_and_append_maersk_data(source_df, final_df, column_mapping, sheet_name="maersk_dsr", default_bond_type=None):
    """Maps and appends data based on column mapping"""
    # Use configured default bond type if not provided
    if default_bond_type is None:
        default_bond_type = config_manager.config["global"]["default_bond_type"]
        
    # Create DataFrame with current target columns
    mapped_df = pd.DataFrame(columns=get_target_columns())
    
    # Rest of the function remains the same...
    # ... [keep all existing code]

    for new_col in final_df.columns:
        if new_col in column_mapping:
            source_col = column_mapping[new_col]
            try:
                if isinstance(source_col, int):
                    mapped_df[new_col] = source_df.iloc[:, source_col]
                else:
                    mapped_df[new_col] = source_df[source_col]
            except (KeyError, IndexError):
                mapped_df[new_col] = pd.NA
        else:
            # Handle special cases
            if new_col == "sheet":
                mapped_df[new_col] = sheet_name
            elif new_col == "HB/L NO":
                mapped_df[new_col] = source_df.get("HBL", pd.NA)
            elif new_col == "Bond or Non Bond":
                mapped_df[new_col] = default_bond_type
            else:
                mapped_df[new_col] = pd.NA

    # Align datatypes before concatenation
    for col in final_df.columns:
        if col in mapped_df:
            mapped_df[col] = mapped_df[col].astype(final_df[col].dtype, errors='ignore')

    # Append and return
    final_df = pd.concat([final_df, mapped_df], ignore_index=True)

    print(f"✅ Appended data from: {sheet_name} | Final shape: {final_df.shape}")
    print(mapped_df[["HBL", "Bond or Non Bond", "sheet", "Origin", "Delivery date"]].head())
    return final_df,mapped_df


# ========================
# EXPO PROCESSING (YOUR EXISTING CODE)
# ========================

def process_expo_file(uploaded_file):
    try:
        # Process Bond Sheet - use get_target_consignees()
        bond_df = pd.read_excel(uploaded_file, sheet_name='Bond')
        matched_bond, _ = filter_and_match_consignee(bond_df, get_target_consignees())
        print(f"🔍 Matched Bond Rows: {len(matched_bond)}")
        processed_bond = process_bond_sheet(matched_bond) if not matched_bond.empty else pd.DataFrame()

        # Process Non-Bond Sheet
        non_bond_df = pd.read_excel(uploaded_file, sheet_name='NON-Bond')
        matched_non_bond, _ = filter_and_match_consignee(non_bond_df, get_target_consignees())
        print(f"🔍 Matched Non-Bond Rows: {len(matched_non_bond)}")
        processed_non_bond = process_non_bond_sheet(matched_non_bond) if not matched_non_bond.empty else pd.DataFrame()

        # Process FCL Sheet
        fcl_df = pd.read_excel(uploaded_file, sheet_name='FCL1')
        matched_fcl, _ = filter_and_match_consignee(fcl_df, get_target_consignees())
        print(f"🔍 Matched FCL Rows: {len(matched_fcl)}")
        processed_fcl = process_fcl_sheet(matched_fcl) if not matched_fcl.empty else pd.DataFrame()

        # Combine all non-empty sheets
        combined_sheets = [df for df in [processed_bond, processed_non_bond, processed_fcl] if not df.empty]

        if not combined_sheets:
            st.warning("⚠️ No matching rows found across any Expo sheets.")
            return pd.DataFrame()

        final_df = pd.concat(combined_sheets, ignore_index=True)

        # Clean up data types - convert potential date columns
        date_cols = ['ETA', 'ATD', 'ATA', 'ETB', 'Estimated Clearance', 'Delivery date']
        for col in date_cols:
            if col in final_df.columns:
                # First try to convert to datetime
                final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
                # Then convert valid dates to date-only format
                final_df[col] = final_df[col].apply(lambda x: x.date() if pd.notna(x) else pd.NA)

        st.session_state.expo_data = final_df
        st.session_state.expo_processed = True

        # --- 💡 Summary Section ---
        st.subheader("📊 Expo Processing Summary")

        total_rows = len(final_df)
        sheet_counts = final_df['sheet'].value_counts()

        st.markdown(f"**✅ Total Rows Processed:** `{total_rows}`")

        cols = st.columns(2)
        with cols[0]:
            st.markdown("**📁 Record Counts by Sheet**")
            st.dataframe(sheet_counts.rename_axis("Sheet").reset_index(name="Records"), 
                         use_container_width=True)

        with cols[1]:
            st.markdown("**📅 ETA Date Ranges**")
            if 'ETA' in final_df.columns:
                eta_stats = final_df[['sheet', 'ETA']].dropna().groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
                st.dataframe(eta_stats, use_container_width=True)
            else:
                st.warning("ETA column not found.")

        # --- 💾 Download Button ---
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download Merged Expo Data",
            data=csv,
            file_name='expo_merged.csv',
            mime='text/csv',
            key='expo_download'
        )

        return final_df

    except Exception as e:
        st.error(f"❌ Error processing Expo file: {str(e)}")
        return pd.DataFrame()

def process_bond_sheet(df):
    # Get mappings from configuration instead of hardcoded
    column_mappings = config_manager.get_mappings("expo", "bond")
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())
    
    # Apply mappings
    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col]
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                print(f"⚠️ Mapping failed for {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "expo_bond"
            elif target_col == "HB/L NO" and "HBL" in df.columns:
                processed_df[target_col] = df["HBL"]
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = "Bond"
            else:
                processed_df[target_col] = pd.NA



    return processed_df



def process_non_bond_sheet(df):
    # Get mappings from configuration
    column_mappings = config_manager.get_mappings("expo", "non_bond")
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())

    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col]
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                print(f"⚠️ Mapping failed for {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "expo_nonbond"
            elif target_col == "HB/L NO" and "HBL" in df.columns:
                processed_df[target_col] = df["HBL"]
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = "Non Bond"
            else:
                processed_df[target_col] = pd.NA

    return processed_df



def process_fcl_sheet(df):
    # Get mappings from configuration
    column_mappings = config_manager.get_mappings("expo", "fcl")
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())

    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col]
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                print(f"⚠️ Mapping failed for {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "expo_fcl"
            elif target_col == "HB/L NO" and "HBL" in df.columns:
                processed_df[target_col] = df["HBL"]
            elif target_col == "Bond or Non Bond":
                processed_df[target_col] = "FCL"
            else:
                processed_df[target_col] = pd.NA

    return processed_df


# ========================
# MAERSK PROCESSING 
# ========================
def process_maersk_file(uploaded_file):
    try:
        st.write("📥 Reading sheets from uploaded file...")
        dsr_df = pd.read_excel(uploaded_file, sheet_name='DSR')
        archived_df = pd.read_excel(uploaded_file, sheet_name='Archieved')


        # Step 1: Filter DSR
        matched_dsr, _ = filter_and_match_consignee(dsr_df, get_target_consignees())
        st.write(f"🔎 Matched DSR Rows: {matched_dsr.shape[0]}")
        if matched_dsr.empty:
            st.warning("⚠️ No matching rows in DSR sheet. Skipping.")
            processed_dsr = pd.DataFrame(columns=get_target_columns())  # Changed
        else:
            processed_dsr = process_maersk_dsr(matched_dsr)

        # Step 2: Filter Archived
        matched_archived, _ = filter_and_match_consignee(archived_df, get_target_consignees())
        st.write(f"🔎 Matched Archived Rows: {matched_archived.shape[0]}")
        if matched_archived.empty:
            st.warning("⚠️ No matching rows in Archived sheet. Skipping.")
            processed_archived = pd.DataFrame(columns=get_target_columns())  # Changed
        else:
            processed_archived = process_maersk_archived(matched_archived)

        # Step 3: Merge and convert dates
        final_df = pd.concat([processed_dsr, processed_archived], ignore_index=True)
        st.write(f"🧩 Combined Final Rows: {final_df.shape[0]}")

        if final_df.empty:
            st.warning("⚠️ Final dataset is empty after processing. No data to show.")
            return pd.DataFrame()  # Returning an empty DataFrame early

        # Convert datetime columns to date
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        st.session_state.maersk_data = final_df
        st.session_state.maersk_processed = True

        # Step 4: Summary + Downloads
        show_maersk_summary(final_df)

        st.subheader("📥 Maersk Data Downloads")
        col1, col2 = st.columns(2)

        with col1:
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Merged Maersk Data",
                data=csv,
                file_name='maersk_combined.csv',
                mime='text/csv',
                key='maersk_download'
            )

        with col2:
            if st.session_state.expo_processed:
                combined = pd.concat([st.session_state.expo_data, final_df], ignore_index=True)
                csv = combined.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Expo+Maersk Combined",
                    data=csv,
                    file_name='expo_maersk_combined.csv',
                    mime='text/csv',
                    key='expo_maersk_download'
                )
            else:
                st.warning("Expo data not processed yet")

        return final_df

    except Exception as e:
        st.error(f"❌ Maersk processing error: {str(e)}")
        print("❌ ERROR TRACE:", e)
        return pd.DataFrame()


def process_maersk_dsr(df):
    # Get mappings from config instead of hardcoded
    column_mappings = config_manager.get_mappings("maersk", "dsr")
    default_bond_type = config_manager.config["global"]["default_bond_type"]
    
    # Create DataFrame with configured columns
    processed_df = pd.DataFrame(columns=get_target_columns())

    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col] if source_col < len(df.columns) else pd.NA
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                st.warning(f"⚠️ Failed to map {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "maersk_dsr"
            elif target_col == "HB/L NO":
                processed_df[target_col] = df.get("HBL", pd.NA)
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = default_bond_type  # From config
            else:
                processed_df[target_col] = pd.NA

    return processed_df



def process_maersk_archived(df):

    column_mappings = config_manager.get_mappings("maersk", "archived")
    default_bond_type = config_manager.config["global"]["default_bond_type"]
    
    processed_df = pd.DataFrame(columns=get_target_columns())
    
    for target_col in get_target_columns():
        if target_col in column_mappings:
            source_col = column_mappings[target_col]
            try:
                if isinstance(source_col, int):
                    processed_df[target_col] = df.iloc[:, source_col] if source_col < len(df.columns) else pd.NA
                elif source_col in df.columns:
                    processed_df[target_col] = df[source_col]
            except Exception as e:
                st.warning(f"⚠️ Failed to map {target_col}: {str(e)}")
                processed_df[target_col] = pd.NA
        else:
            # Handle special cases
            if target_col == "sheet":
                processed_df[target_col] = "maersk_archived"
            elif target_col == "HB/L NO":
                processed_df[target_col] = df.get("HBL", pd.NA)
            # elif target_col == "Bond or Non Bond":
            #     processed_df[target_col] = pd.NA  # From config
            else:
                processed_df[target_col] = pd.NA

    return processed_df



def show_maersk_summary(final_df):
    st.subheader("📊 Maersk Processing Summary")
    
    total_rows = len(final_df)
    sheet_counts = final_df['sheet'].value_counts()

    # 👉 Ensure 'ETA' is in datetime format before grouping
    if 'ETA' in final_df.columns:
        try:
            final_df['ETA'] = pd.to_datetime(final_df['ETA'], errors='coerce')
        except Exception as e:
            st.warning(f"⚠️ Failed to convert ETA to datetime: {str(e)}")

    # Now safely aggregate
    if 'ETA' in final_df.columns and pd.api.types.is_datetime64_any_dtype(final_df['ETA']):
        eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
        
        st.markdown("**📅 Date Ranges**")
        st.dataframe(eta_stats, hide_index=True, use_container_width=True)
    else:
        st.warning("⚠️ ETA column missing or not in datetime format. Skipping date ranges.")

    cols = st.columns(3)
    cols[0].metric("Total Rows", total_rows)
    cols[1].metric("DSR Records", sheet_counts.get('maersk_dsr', 0))
    cols[2].metric("Archived Records", sheet_counts.get('maersk_archived', 0))


# ========================
# GLOBE ACTIVE PROCESSING 
# ========================

def process_globe_file(uploaded_file):
    try:
        st.write("📥 Reading sheets from uploaded file...")
        ongoing_df = pd.read_excel(uploaded_file, sheet_name='ONGOING')
        cleared_df = pd.read_excel(uploaded_file, sheet_name='CLEARED')

        # Get configurations
        target_consignees = get_target_consignees()
        ongoing_mappings = config_manager.get_mappings("globe", "ongoing")
        cleared_mappings = config_manager.get_mappings("globe", "cleared")
        default_bond_type = config_manager.config["global"]["default_bond_type"]

        # Rest of your processing code...
        # Initialize final DataFrame
        final_df = pd.DataFrame(columns=get_target_columns())
        
        # Process ONGOING sheet - MODIFIED SECTION
        st.write("🔎 Processing ONGOING sheet...")
        ongoing_matched, _ = process_consignee_matching(
            df=ongoing_df,
            target_consignees=target_consignees,
            date_column="ETA DATE",
            consignee_column="CONSIGNEE"
        )
        
        if not ongoing_matched.empty:
            # Remove temporary columns added by process_consignee_matching
            ongoing_matched = ongoing_matched.drop(columns=['Consignee_clean', 'BestMatch', 'Score', 'ETA Remark'], errors='ignore')
            # Restore original column names
            ongoing_matched = ongoing_matched.rename(columns={
                'ETA': 'ETA DATE',
                'Consignee': 'CONSIGNEE'
            })
            
            final_df, mapped_ongoing = map_and_append_maersk_data(
                source_df=ongoing_matched,
                final_df=final_df,
                column_mapping=ongoing_mappings,
                sheet_name="globe_ongoing",
                # default_bond_type="FCL"
            )
            st.write(f"✅ Added {len(mapped_ongoing)} ONGOING records")
        else:
            st.warning("⚠️ No matching ONGOING records found")

        # Process CLEARED sheet - SAME MODIFICATION
        st.write("🔎 Processing CLEARED sheet...")
        cleared_matched, _ = process_consignee_matching(
            df=cleared_df,
            target_consignees=target_consignees,
            date_column="ETA DATE",
            consignee_column="CONSIGNEE"
        )
        
        if not cleared_matched.empty:
            cleared_matched = cleared_matched.drop(columns=['Consignee_clean', 'BestMatch', 'Score', 'ETA Remark'], errors='ignore')
            cleared_matched = cleared_matched.rename(columns={
                'ETA': 'ETA DATE',
                'Consignee': 'CONSIGNEE'
            })
            
            final_df, mapped_cleared = map_and_append_maersk_data(
                source_df=cleared_matched,
                final_df=final_df,
                column_mapping=cleared_mappings,
                sheet_name="globe_cleared",
            )
            st.write(f"✅ Added {len(mapped_cleared)} CLEARED records")
        else:
            st.warning("⚠️ No matching CLEARED records found")

        # Convert datetime columns
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        # Store in session state
        st.session_state.globe_data = final_df
        st.session_state.globe_processed = True

        # Show summary
        show_globe_summary(final_df)

        # Download options
        st.subheader("📥 Globe Active Data Downloads")
        col1, col2 = st.columns(2)
        
        with col1:
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Globe Data",
                data=csv,
                file_name='globe_active.csv',
                mime='text/csv',
                key='globe_download'
            )
        
        with col2:
            if st.session_state.expo_processed and st.session_state.maersk_processed:
                combined = pd.concat([
                    st.session_state.expo_data,
                    st.session_state.maersk_data,
                    final_df
                ], ignore_index=True)
                csv = combined.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Combined Data",
                    data=csv,
                    file_name='all_combined.csv',
                    mime='text/csv',
                    key='combined_download'
                )
            else:
                st.warning("Requires processed Expo and Maersk data")

        return final_df

    except Exception as e:
        st.error(f"❌ Globe Active processing error: {str(e)}")
        return pd.DataFrame()

def show_globe_summary(final_df):
    st.subheader("📊 Globe Active Summary")
    
    if final_df.empty:
        st.warning("No data available")
        return
    
    # Metrics
    total = len(final_df)
    ongoing = len(final_df[final_df['sheet'] == 'globe_ongoing'])
    cleared = len(final_df[final_df['sheet'] == 'globe_cleared'])
    
    cols = st.columns(3)
    cols[0].metric("Total Records", total)
    cols[1].metric("Ongoing Shipments", ongoing)
    cols[2].metric("Cleared Shipments", cleared)
    
    # Date ranges
    st.write("\n")
    st.markdown("**📅 ETA Date Ranges**")
    eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
    st.dataframe(eta_stats, use_container_width=True)

# ========================
# SCANWELL PROCESSING 
# ========================
def process_scanwell_file(uploaded_file):
    try:
        st.write("📥 Reading sheets from uploaded file...")
        unichela_df = pd.read_excel(uploaded_file, sheet_name='UNICHELA -2025')
        bodyline_df = pd.read_excel(uploaded_file, sheet_name='BODYLINE-2025')

        target_consignees = get_target_consignees()
        unichela_mappings = config_manager.get_mappings("scanwell", "unichela")
        bodyline_mappings = config_manager.get_mappings("scanwell", "bodyline")

        # Initialize final DataFrame
        final_df = pd.DataFrame(columns=get_target_columns())
        
     
        # Process UNICHELA sheet
        st.write("🔎 Processing UNICHELA sheet...")
        unichela_matched, _ = process_consignee_matching(
            df=unichela_df,
            target_consignees=target_consignees,
            date_column="ETA",
            consignee_column="Consignee"
        )
        
        if not unichela_matched.empty:
            final_df, mapped_unichela = map_and_append_maersk_data(
                source_df=unichela_matched,
                final_df=final_df,
                column_mapping=unichela_mappings,
                sheet_name="scanwell_unichela"
            )
            st.write(f"✅ Added {len(mapped_unichela)} UNICHELA records")
        else:
            st.warning("⚠️ No matching UNICHELA records found")

        # Process BODYLINE sheet
        st.write("🔎 Processing BODYLINE sheet...")
        bodyline_matched, _ = process_consignee_matching(
            df=bodyline_df,
            target_consignees=target_consignees,
            date_column="ETA",
            consignee_column="Consignee"
        )
        
        if not bodyline_matched.empty:
            final_df, mapped_bodyline = map_and_append_maersk_data(
                source_df=bodyline_matched,
                final_df=final_df,
                column_mapping=bodyline_mappings,
                sheet_name="scanwell_bodyline"
            )
            st.write(f"✅ Added {len(mapped_bodyline)} BODYLINE records")
        else:
            st.warning("⚠️ No matching BODYLINE records found")

        # Convert datetime columns
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        # Store in session state
        st.session_state.scanwell_data = final_df
        st.session_state.scanwell_processed = True

        # Show summary
        show_scanwell_summary(final_df)

        # Download options
        st.subheader("📥 Scanwell Data Downloads")
        col1, col2 = st.columns(2)
        
        with col1:
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Scanwell Data",
                data=csv,
                file_name='scanwell_combined.csv',
                mime='text/csv',
                key='scanwell_download'
            )
        
        with col2:
            if st.session_state.expo_processed and st.session_state.maersk_processed and st.session_state.globe_processed:
                combined = pd.concat([
                    st.session_state.expo_data,
                    st.session_state.maersk_data,
                    st.session_state.globe_data,
                    final_df
                ], ignore_index=True)
                csv = combined.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Combined Data",
                    data=csv,
                    file_name='all_combined.csv',
                    mime='text/csv',
                    key='combined_download'
                )
            else:
                st.warning("Requires processed Expo, Maersk and Globe data")

        return final_df

    except Exception as e:
        st.error(f"❌ Scanwell processing error: {str(e)}")
        return pd.DataFrame()

def show_scanwell_summary(final_df):
    st.subheader("📊 Scanwell Summary")
    
    if final_df.empty:
        st.warning("No data available")
        return
    
    # Metrics
    total = len(final_df)
    unichela = len(final_df[final_df['sheet'] == 'scanwell_unichela'])
    bodyline = len(final_df[final_df['sheet'] == 'scanwell_bodyline'])
    
    cols = st.columns(3)
    cols[0].metric("Total Records", total)
    cols[1].metric("Unichela Shipments", unichela)
    cols[2].metric("Bodyline Shipments", bodyline)
    
    # Date ranges
    st.write("\n")
    st.markdown("**📅 ETA Date Ranges**")
    eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
    st.dataframe(eta_stats, use_container_width=True)

# ========================
# UI COMPONENTS
# ========================
def show_sidebar():
    with st.sidebar:
        #st.image("https://via.placeholder.com/150x50?text=DSR+Processor", width=150)  # Add your logo here
        
        # Configuration Section
        config_expander = st.expander("⚙️ CONFIGURATION", expanded=False)
        with config_expander:
            if st.button("🛠️ Open Settings Panel"):
                st.session_state.show_config = not st.session_state.get('show_config', False)
            
            if st.session_state.get('show_config', False):
                show_configuration_ui()
        
        st.markdown("---")
        st.subheader("🛠️ Processing Parameters")
        
        # Add date filtering toggle
        st.session_state.use_date_filter = st.checkbox(
            "Enable Date Filtering",
            value=False,
            help="Filter records by date range when enabled"
        )
        
        # Only show date selection if filtering is enabled
        if st.session_state.use_date_filter:
            weeks = config_manager.config["global"]["date_range_weeks"]
            selected_date = st.date_input(
                "📅 Reference Date for Shipments",
                value=st.session_state.selected_reference_date,
                help=f"Will show shipments within ±{weeks} weeks of this date"
            )
            st.session_state.selected_reference_date = pd.Timestamp(selected_date)
        
        # Fuzzy Threshold with visual indicator
        threshold = config_manager.config["global"]["fuzzy_threshold"]
        st.progress(threshold/100, text=f"🔍 Fuzzy Matching Threshold: {threshold}%")
        
        st.markdown("---")
        st.subheader("📋 Processing Pipeline")
        
        # Enhanced progress tracker
        steps = [
            (1, "1. Expo Data", st.session_state.expo_processed),
            (2, "2. Maersk Data", st.session_state.maersk_processed),
            (3, "3. Globe Data", st.session_state.globe_processed),
            (4, "4. Scanwell Data", st.session_state.scanwell_processed),
            (5, "5. Final Results", all([
                st.session_state.expo_processed,
                st.session_state.maersk_processed,
                st.session_state.globe_processed,
                st.session_state.scanwell_processed
            ]))
        ]
        
        for step_num, step_name, is_complete in steps:
            if st.session_state.current_step == step_num:
                st.markdown(f"➡️ **{step_name}** (Current Step)")
            elif is_complete:
                st.markdown(f"✅ ~~{step_name}~~ (Completed)")
            else:
                st.markdown(f"◻️ {step_name}")
        
        # Navigation controls with better icons
        if st.session_state.current_step > 1 and st.session_state.current_step < 5:
            if st.button("⏮️ Previous Step"):
                st.session_state.current_step -= 1
                st.rerun()
        
        # Final step actions
        if st.session_state.current_step == 5:
            st.markdown("---")
            st.subheader("📤 Export Results")
            combined = pd.concat([
                st.session_state.expo_data,
                st.session_state.maersk_data,
                st.session_state.globe_data,
                st.session_state.scanwell_data
            ], ignore_index=True)
            
            csv = combined.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="💾 Download Complete Dataset",
                data=csv,
                file_name='dsr_combined_results.csv',
                mime='text/csv',
                help="Download all processed data as a single CSV file"
            )
            
            if st.button("🔄 Start New Processing Run", type="primary"):
                init_session_state()
                st.rerun()

def show_current_step():
    st.title("📊 Piyadasa-DSR Data Processor")
    st.caption("Process and combine shipment data from multiple sources")
    
    # Add a button to jump directly to visualization step
    if st.session_state.current_step != 5:
        if st.button("🚀 Jump to Visualization Step", type="secondary"):
            st.session_state.current_step = 5
            st.rerun()
    
    current_step = st.session_state.current_step

    # Step 1: Expo Data
    if current_step == 1:
        with st.container(border=True):
            st.subheader("1. Upload Expo Data")
            uploaded_file = st.file_uploader(
                "Select Expo Excel File", 
                type=['xlsx'],
                key='expo_upload',
                help="Upload the DSR MAS Excel file with Bond/NON-Bond/FCL sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Expo data...'):
                    result = process_expo_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Expo data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ Continue to Maersk Data", type="primary"):
                            st.session_state.current_step = 2
                            st.rerun()
    
    # Step 2: Maersk Data
    elif current_step == 2:
        with st.container(border=True):
            st.subheader("2. Upload Maersk Data")
            uploaded_file = st.file_uploader(
                "Select Maersk Excel File", 
                type=['xlsx'],
                key='maersk_upload',
                help="Upload the Maersk Excel file with DSR/Archived sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Maersk data...'):
                    result = process_maersk_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Maersk data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ Continue to Globe Data", type="primary"):
                            st.session_state.current_step = 3
                            st.rerun()
    
    # Step 3: Globe Data
    elif current_step == 3:
        with st.container(border=True):
            st.subheader("3. Upload Globe Active Data")
            uploaded_file = st.file_uploader(
                "Select Globe Excel File", 
                type=['xlsx'],
                key='globe_upload',
                help="Upload the Globe Active Excel file with ONGOING/CLEARED sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Globe data...'):
                    result = process_globe_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Globe data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ Continue to Scanwell Data", type="primary"):
                            st.session_state.current_step = 4
                            st.rerun()
    
    # Step 4: Scanwell Data
    elif current_step == 4:
        with st.container(border=True):
            st.subheader("4. Upload Scanwell Data")
            uploaded_file = st.file_uploader(
                "Select Scanwell Excel File", 
                type=['xls', 'xlsx'],
                key='scanwell_upload',
                help="Upload the Scanwell Excel file with UNICHELA/BODYLINE sheets"
            )
            
            if uploaded_file:
                with st.spinner('🔍 Processing Scanwell data...'):
                    result = process_scanwell_file(uploaded_file)
                    if not result.empty:
                        st.success("✅ Scanwell data processed successfully!")
                        with st.expander("👀 Preview Processed Data"):
                            st.dataframe(result.head())
                        
                        if st.button("➡️ View Final Results", type="primary"):
                            st.session_state.current_step = 5
                            st.rerun()
    # Final Step: Results
    elif current_step == 5:
        with st.container(border=True):
            st.subheader("📊 Data Visualization & Analysis")
            
            # Initialize combined as empty DataFrame
            combined = pd.DataFrame()
            
            # Add option to use existing merged data or upload new
            analysis_option = st.radio(
                "Choose data source:",
                options=["Use previously processed data", "Upload new file for analysis"],
                index=0,
                horizontal=True
            )
            
            if analysis_option == "Upload new file for analysis":
                quick_file = st.file_uploader(
                    "Upload any Excel/CSV for analysis",
                    type=['xlsx', 'xls', 'csv'],
                    key='quick_analysis'
                )
                
                if quick_file:
                    try:
                        if quick_file.name.endswith('.csv'):
                            combined = pd.read_csv(quick_file)
                        else:
                            combined = pd.read_excel(quick_file)
                        st.success("File uploaded successfully!")
                    except Exception as e:
                        st.error(f"Error reading file: {str(e)}")
            else:
                # Use existing processed data
                if (st.session_state.expo_processed or st.session_state.maersk_processed or 
                    st.session_state.globe_processed or st.session_state.scanwell_processed):
                    combined = pd.concat([
                        st.session_state.expo_data,
                        st.session_state.maersk_data,
                        st.session_state.globe_data,
                        st.session_state.scanwell_data
                    ], ignore_index=True)
                else:
                    st.warning("No processed data available - please upload files in previous steps")
            
            # Only proceed if we have data
            if not combined.empty:
                st.success(f"✨ Data loaded! Total records: {len(combined):,}")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Unique HBLs", combined['HBL'].nunique())
                with col2:
                    # Normalize column names
                    combined.columns = [col.strip() for col in combined.columns]
                    gross_weight_col = next(
                        (col for col in combined.columns if col.lower().replace(" ", "") == "grossweight"), 
                        None
                    )
                    if gross_weight_col:
                        try:
                            combined[gross_weight_col] = (
                                pd.to_numeric(
                                    combined[gross_weight_col]
                                    .astype(str)
                                    .str.replace(",", "")
                                    .str.extract(r"(\d+\.?\d*)")[0],
                                    errors='coerce'
                                )
                            )
                            total_weight = combined[gross_weight_col].sum()
                            st.metric("Total Gross Weight", f"{total_weight:,.2f} kg")
                        except Exception as e:
                            st.metric("Total Gross Weight", f"Error: {str(e)}")
                    else:
                        st.metric("Total Gross Weight", "Column not found")

                # Filter block
                st.markdown("### 🔎 Filter Records")
                filter_col1, filter_col2 = st.columns(2)

                with filter_col1:
                    selected_hbls = st.multiselect(
                        "Filter by HBL",
                        options=combined['HBL'].dropna().unique(),
                        default=None
                    )

                with filter_col2:
                    selected_invs = st.multiselect(
                        "Filter by Inv #",
                        options=combined['Inv #'].dropna().unique(),
                        default=None
                    )

                filtered_combined = combined.copy()
                if selected_hbls:
                    filtered_combined = filtered_combined[filtered_combined['HBL'].isin(selected_hbls)]
                if selected_invs:
                    filtered_combined = filtered_combined[filtered_combined['Inv #'].isin(selected_invs)]

                st.info(f"🔍 Showing {len(filtered_combined):,} filtered records")

                # Show filtered data
                if not filtered_combined.empty:
                    with st.expander("🔍 View Filtered Data", expanded=False):
                        st.dataframe(
                            filtered_combined,
                            use_container_width=True,
                            height=400
                        )

                # Visualizations organized in tabs
                st.markdown("---")
                st.subheader("📊 Additional Visual Insights")
                
                # Initialize date filter variables
                viz_filtered = filtered_combined.copy()
                
                if 'ETA' in filtered_combined.columns:
                    try:
                        # Ensure ETA is datetime and drop NA values
                        filtered_combined['ETA'] = pd.to_datetime(filtered_combined['ETA'], errors='coerce')
                        valid_dates = filtered_combined.dropna(subset=['ETA'])
                        
                        if not valid_dates.empty:
                            min_date = valid_dates['ETA'].min().date()
                            max_date = valid_dates['ETA'].max().date()
                            
                            # Initialize session state for dates
                            if 'date_filter' not in st.session_state:
                                st.session_state.date_filter = {
                                    'start_date': min_date,
                                    'end_date': max_date
                                }
                            
                            # Date filter section for visualizations
                            st.markdown("### ⏳ Filter Visualizations by Date Range")
                            date_col1, date_col2, date_col3 = st.columns([2, 2, 1])
                            
                            with date_col1:
                                start_date = st.date_input(
                                    "Start Date",
                                    value=st.session_state.date_filter['start_date'],
                                    min_value=min_date,
                                    max_value=max_date,
                                    key='viz_start_date'
                                )
                            
                            with date_col2:
                                end_date = st.date_input(
                                    "End Date",
                                    value=st.session_state.date_filter['end_date'],
                                    min_value=min_date,
                                    max_value=max_date,
                                    key='viz_end_date'
                                )
                            
                            with date_col3:
                                st.write("")  # Spacer for alignment
                                if st.button("🔄 Reset Dates", key="reset_viz_dates"):
                                    st.session_state.date_filter = {
                                        'start_date': min_date,
                                        'end_date': max_date
                                    }
                                    st.rerun()
                            
                            # Update session state if dates changed
                            if (start_date != st.session_state.date_filter['start_date'] or 
                                end_date != st.session_state.date_filter['end_date']):
                                st.session_state.date_filter = {
                                    'start_date': start_date,
                                    'end_date': end_date
                                }
                                st.rerun()
                            
                            # Apply date filter to all visualizations
                            viz_filtered = filtered_combined[
                                (filtered_combined['ETA'].dt.date >= st.session_state.date_filter['start_date']) & 
                                (filtered_combined['ETA'].dt.date <= st.session_state.date_filter['end_date'])
                            ]
                            
                            # Show record count instead of date range
                            st.info(f"📊 Showing {len(viz_filtered):,} records")
                        else:
                            st.warning("No valid dates available in ETA column")
                            viz_filtered = filtered_combined
                            
                    except Exception as e:
                        st.warning(f"Couldn't process dates: {e}")
                        viz_filtered = filtered_combined
                else:
                    viz_filtered = filtered_combined
                
                # Visualization tabs - ALL using viz_filtered with proper formatting
                tab1, tab2, tab3, tab4 = st.tabs([
                    "📅 Time Trends", 
                    "🚢 Vessel Insights", 
                    "🏢 SBU Insights", 
                    "🌍 Origin Insights"
                ])

                with tab1:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'ETA' in viz_filtered.columns and 'sheet' in viz_filtered.columns:
                            try:
                                eta_df = viz_filtered.dropna(subset=['ETA', 'sheet'])
                                if not eta_df.empty:
                                    eta_df['ETA_Date'] = eta_df['ETA'].dt.date
                                    eta_trend_df = eta_df.groupby(['sheet', 'ETA_Date']).size().reset_index(name='Bookings')
                                    
                                    fig_eta = px.line(
                                        eta_trend_df,
                                        x='ETA_Date',
                                        y='Bookings',
                                        color='sheet',
                                        title='📆 Bookings Over Time by Sheet',
                                        markers=True,
                                        labels={'ETA_Date': 'ETA Date', 'Bookings': 'Number of Bookings'}
                                    )
                                    fig_eta.update_layout(
                                        paper_bgcolor='rgba(0,0,0,0)',
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        font=dict(color='white' if st.session_state.dark_mode else 'black'),
                                        height=500
                                    )
                                    st.plotly_chart(fig_eta, use_container_width=True)
                                else:
                                    st.warning("No data available after filtering")
                            except Exception as e:
                                st.warning(f"Couldn't generate ETA trend chart: {e}")
                    
                    with col2:
                        if 'Delivery date' in viz_filtered.columns:
                            try:
                                delivery_df = viz_filtered.copy()
                                
                                # Exclude 'globe_cleared' rows if the column exists
                                if 'globe_cleared' in delivery_df.columns:
                                    delivery_df = delivery_df[~delivery_df['globe_cleared'].notna()]
                                
                                # Determine delivery status
                                delivery_df['Status'] = 'Pending'  # Default to pending
                                delivery_df.loc[delivery_df['Delivery date'].notna(), 'Status'] = 'Delivered'
                                
                                # Include 'globe_ongoing' in pending if the column exists
                                if 'globe_ongoing' in delivery_df.columns:
                                    delivery_df.loc[delivery_df['globe_ongoing'].notna(), 'Status'] = 'Pending'
                                
                                # Count statuses
                                delivery_summary = delivery_df['Status'].value_counts().reset_index()
                                delivery_summary.columns = ['Status', 'Count']
                                
                                # Create the plot
                                fig_delivery = px.bar(
                                    delivery_summary,
                                    x='Status',
                                    y='Count',
                                    color='Status',
                                    title='🚚 Delivery Status Breakdown',
                                    text='Count',
                                    category_orders={"Status": ["Delivered", "Pending"]}  # Ensures consistent order
                                )
                                
                                fig_delivery.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black'),
                                    xaxis_title="Delivery Status",
                                    yaxis_title="Number of Orders"
                                )
                                
                                # Update color mapping if needed
                                color_map = {'Delivered': 'green', 'Pending': 'orange'}
                                fig_delivery.for_each_trace(lambda t: t.update(marker_color=color_map[t.name]))
                                
                                st.plotly_chart(fig_delivery, use_container_width=True)
                                
                            except Exception as e:
                                st.warning(f"Couldn't generate delivery status chart: {e}")
                with tab2:
                    # Vessel insights using viz_filtered with proper formatting
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'Origin Vessel' in viz_filtered.columns:
                            try:
                                vessel_counts = viz_filtered['Origin Vessel'].value_counts().head(10).reset_index()
                                vessel_counts.columns = ['Vessel', 'Count']
                                fig_vessel = px.bar(
                                    vessel_counts,
                                    x='Vessel',
                                    y='Count',
                                    color='Vessel',
                                    title='Top 10 Vessels by Shipment Count'
                                )
                                fig_vessel.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black')
                                )
                                st.plotly_chart(fig_vessel, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Couldn't generate vessel chart: {e}")
                    
                    with col2:
                        if 'Origin Vessel' in viz_filtered.columns and 'Gross Weight' in viz_filtered.columns:
                            try:
                                vessel_weight = viz_filtered.groupby('Origin Vessel')['Gross Weight'].sum().head(6).reset_index()
                                vessel_weight.columns = ['Vessel', 'Total Weight']
                                fig_weight = px.pie(
                                    vessel_weight,
                                    names='Vessel',
                                    values='Total Weight',
                                    title='Weight Distribution by Vessel (Top 6)'
                                )
                                fig_weight.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black')
                                )
                                st.plotly_chart(fig_weight, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Couldn't generate weight chart: {e}")

                with tab3:
                    # SBU insights using viz_filtered with proper formatting
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'SBU' in viz_filtered.columns:
                            try:
                                sbu_counts = viz_filtered['SBU'].value_counts().reset_index()
                                sbu_counts.columns = ['SBU', 'Count']
                                fig_sbu = px.bar(
                                    sbu_counts,
                                    x='SBU',
                                    y='Count',
                                    color='SBU',
                                    title='Shipments by SBU'
                                )
                                fig_sbu.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black')
                                )
                                st.plotly_chart(fig_sbu, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Couldn't generate SBU chart: {e}")
                    
                    with col2:
                        if 'SBU' in viz_filtered.columns and 'Gross Weight' in viz_filtered.columns:
                            try:
                                sbu_weight = viz_filtered.groupby('SBU')['Gross Weight'].sum().reset_index()
                                sbu_weight.columns = ['SBU', 'Total Weight']
                                fig_sbu_weight = px.treemap(
                                    sbu_weight,
                                    path=['SBU'],
                                    values='Total Weight',
                                    title='Weight Distribution by SBU'
                                )
                                fig_sbu_weight.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black')
                                )
                                st.plotly_chart(fig_sbu_weight, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Couldn't generate SBU weight chart: {e}")

                with tab4:
                    # Origin insights using viz_filtered with proper formatting
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'Origin' in viz_filtered.columns:
                            try:
                                origin_counts = viz_filtered['Origin'].value_counts().head(10).reset_index()
                                origin_counts.columns = ['Origin', 'Count']
                                fig_origin = px.bar(
                                    origin_counts,
                                    x='Origin',
                                    y='Count',
                                    color='Origin',
                                    title='Top 10 Origins by Shipment Count'
                                )
                                fig_origin.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black')
                                )
                                st.plotly_chart(fig_origin, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Couldn't generate origin chart: {e}")
                    
                    with col2:
                        if 'Origin' in viz_filtered.columns and 'Shipper' in viz_filtered.columns:
                            try:
                                origin_shipper = viz_filtered.groupby(['Origin', 'Shipper']).size().reset_index(name='Count')
                                fig_origin_shipper = px.sunburst(
                                    origin_shipper,
                                    path=['Origin', 'Shipper'],
                                    values='Count',
                                    title='Shipper Distribution by Origin'
                                )
                                fig_origin_shipper.update_layout(
                                    height=500,
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    font=dict(color='white' if st.session_state.dark_mode else 'black')
                                )
                                st.plotly_chart(fig_origin_shipper, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Couldn't generate origin-shipper chart: {e}")


def show_summary_statistics(df, title):
    """Helper function to show summary statistics for any DataFrame"""
    st.write(f"**{title} Summary Statistics**")
    
    if df.empty:
        st.warning("No data available for summary")
        return
    
    # Basic stats
    cols = st.columns(3)
    cols[0].metric("Total Records", len(df))
    cols[1].metric("Columns", len(df.columns))
    
    # Show numeric columns stats
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        st.write("**Numeric Columns Statistics**")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    # Show date columns stats if any
    date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    if len(date_cols) > 0:
        st.write("**Date Ranges**")
        date_stats = []
        for col in date_cols:
            min_date = df[col].min()
            max_date = df[col].max()
            date_stats.append({
                "Column": col,
                "Earliest": min_date if pd.notna(min_date) else "N/A",
                "Latest": max_date if pd.notna(max_date) else "N/A"
            })
        st.dataframe(pd.DataFrame(date_stats), use_container_width=True)
    
    # Show value counts for categorical columns
    cat_cols = [col for col in df.columns if df[col].nunique() < 20 and df[col].nunique() > 1]
    if len(cat_cols) > 0:
        st.write("**Category Distributions**")
        for col in cat_cols:
            st.write(f"**{col}**")
            st.bar_chart(df[col].value_counts())



def main():
    st.set_page_config(
        page_title="DSR Processor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize dark mode state
    if 'dark_mode' not in st.session_state:
        st.session_state.dark_mode = False
    
    # Add dark mode toggle to sidebar (will appear above other sidebar elements)
    with st.sidebar:
        dark_mode = st.toggle("🌙 Dark Mode", value=st.session_state.dark_mode)
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
    
    # Apply appropriate CSS based on mode
    if st.session_state.dark_mode:
        dark_css = """
        <style>
            /* Dark mode styles */
            .stApp {
                background-color: #0E1117;
                color: #FAFAFA;
            }
            [data-testid="stSidebar"] {
                background-color: #0E1117 !important;
            }
            .stMarkdown, .stText, h1, h2, h3, h4, h5, h6 {
                color: #FAFAFA !important;
            }
            .dataframe {
                background-color: #0E1117 !important;
                color: #FAFAFA !important;
            }
            [data-testid="stExpander"] {
                background: rgba(30, 30, 30, 0.8) !important;
                border-color: #444 !important;
            }
            /* Your existing button styles - modified for dark mode */
            .stButton>button {
                transition: all 0.3s ease;
                background-color: #4F8BF9;
                color: white !important;
            }
            .stButton>button:hover {
                transform: scale(1.02);
                background-color: #3a7de9;
            }
            .stProgress>div>div>div {
                background-color: #4CAF50;
            }
        </style>
        """
        st.markdown(dark_css, unsafe_allow_html=True)
    else:
        # Light mode CSS (your original styles)
        st.markdown("""
        <style>
            .stButton>button {
                transition: all 0.3s ease;
            }
            .stButton>button:hover {
                transform: scale(1.02);
            }
            .stProgress>div>div>div {
                background-color: #4CAF50;
            }
            [data-testid="stExpander"] {
                background: rgba(245, 245, 245, 0.1);
                border-radius: 8px;
            }
        </style>
        """, unsafe_allow_html=True)
    
    if 'config_manager' not in st.session_state:
        st.session_state.config_manager = ConfigurationManager()
    
    show_sidebar()
    show_current_step()
    
    if st.session_state.get('show_config', False):
        errors = st.session_state.config_manager.validate_config()
        if errors:
            st.toast("⚠️ Configuration errors detected!", icon="⚠️")
            with st.expander("Configuration Issues", expanded=True):
                for error in errors:
                    st.error(error)

if __name__ == "__main__":
    main()


