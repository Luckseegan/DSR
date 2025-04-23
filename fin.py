import streamlit as st
import pandas as pd
from datetime import timedelta
from rapidfuzz import process, fuzz
import time


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
                "Delivery date", "Delivery location", "sheet"
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
                                "Type": " Type (In two letters)", "SBU": "Consignee"
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
                            "SBU": "Consignee"
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
                                "Type": 14, "SBU": "Consignee"
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
        "SBU": "Consignee"
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
        "SBU": "Consignee"
    }
                    },
                    "globe": {
                        "ongoing": {
                                    # Exact matches (auto-aligned columns)
                                    "ETA": "ETA",
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
                                    "Voyage No": None,  # Assuming Voyage No is missing
                                },
                        "cleared":  {
                                        # Exact matches (auto-aligned columns)
                                        "ETA": "ETA",
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
                                        "Voyage No": None,  # Assuming Voyage No is missing
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
                                "Shipper": "Supplier", "Status": "Pre Alert Status", "Type": "CTN Type"
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
                            "Shipper": "Supplier", "Status": "Pre Alert Status", "Type": "CTN Type"
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

    # def validate_config(self):
    #     """Validate the current configuration"""
    #     errors = []
        
        
    #     # Check mappings reference valid columns
    #     for file_type, sheets in self.config["mappings"].items():
    #         for sheet_type, mappings in sheets.items():
    #             for target_col in mappings.keys():
    #                 if target_col not in self.get_all_columns():
    #                     errors.append(f"Mapping references non-existent column: {target_col} in {file_type}/{sheet_type}")
        
    #     return errors

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


# def show_column_management():
#     st.subheader("Column Management")
    
#     # Current columns display
#     col1, col2 = st.columns(2)
#     with col1:
#         st.write("**Required Columns**")
#         required_cols = st.data_editor(
#             pd.DataFrame(config_manager.config["columns"]["required"], columns=["Column"]),
#             num_rows="dynamic",
#             key="required_cols_editor",
#             hide_index=True
#         )
        
#     with col2:
#         st.write("**Optional Columns**")
#         optional_cols = st.data_editor(
#             pd.DataFrame(config_manager.config["columns"]["optional"], columns=["Column"]),
#             num_rows="dynamic",
#             key="optional_cols_editor",
#             hide_index=True
#         )
    
#     # Add new column
#     st.subheader("Add New Column")
#     new_col = st.text_input("Column Name", key="new_column_name")
#     col_type = st.radio("Column Type", ["Optional", "Required"], key="new_column_type")
    
#     if st.button("Add Column", key="add_column_btn"):
#         if not new_col.strip():
#             st.error("Column name cannot be empty")
#         elif config_manager.add_column(new_col.strip(), col_type == "Required"):
#             st.success(f"Added {col_type.lower()} column: {new_col}")
#             config_manager.save_config()
#             st.rerun()
#         else:
#             st.error(f"Column '{new_col}' already exists")
    
#     # Remove column
#     st.subheader("Remove Column")
#     all_optional_cols = config_manager.config["columns"]["optional"]
#     col_to_remove = st.selectbox(
#         "Select column to remove",
#         [""] + all_optional_cols,
#         key="col_to_remove"
#     )
    
#     if col_to_remove and st.button("Remove Column", key="remove_column_btn"):
#         if config_manager.remove_column(col_to_remove):
#             st.success(f"Removed column: {col_to_remove}")
#             config_manager.save_config()
#             st.rerun()
#         else:
#             st.error(f"Cannot remove required column: {col_to_remove}")

# def show_mapping_management():
#     st.subheader("Mapping Management")
    
#     file_type = st.selectbox(
#         "Select File Type",
#         ["expo", "maersk", "globe", "scanwell"],
#         key="mapping_file_type"
#     )
    
#     sheet_type = st.selectbox(
#         "Select Sheet Type",
#         list(config_manager.config["mappings"].get(file_type, {}).keys()),
#         key="mapping_sheet_type"
#     )
    
#     if not sheet_type:
#         st.warning(f"No sheets defined for {file_type}")
#         return
    
#     current_mappings = config_manager.get_mappings(file_type, sheet_type)
#     updated_mappings = {}
    
#     for i, target_col in enumerate(config_manager.get_all_columns()):
#         current_val = current_mappings.get(target_col, "")
        
#         col1, col2 = st.columns([1, 3])
#         with col1:
#             st.markdown(f"**{target_col}**")
        
#         with col2:
#             # Use index in key to guarantee uniqueness
#             updated_mappings[target_col] = st.text_input(
#                 "Source column or index",
#                 value=current_val,
#                 key=f"mapping_{file_type}_{sheet_type}_{i}",  # Unique index-based key
#                 label_visibility="collapsed"
#             )
    
#     if st.button("Save Mappings", key=f"save_{file_type}_{sheet_type}"):
#         config_manager.update_mappings(
#             file_type,
#             sheet_type,
#             {k: v for k, v in updated_mappings.items() if v}
#         )
#         if config_manager.save_config():
#             st.success("Mappings saved!")

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

def filter_and_match_consignee(df, target_consignees=None, date_column="ETA", threshold=None):
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

        # Rest of the function remains exactly the same...
        # ... [keep all existing code]
        # Combine all non-empty sheets
        combined_sheets = [df for df in [processed_bond, processed_non_bond, processed_fcl] if not df.empty]

        if not combined_sheets:
            st.warning("⚠️ No matching rows found across any Expo sheets.")
            return pd.DataFrame()

        final_df = pd.concat(combined_sheets, ignore_index=True)

        # Convert all datetime columns to date only (no time)
        for col in final_df.columns:
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.date

        st.session_state.expo_data = final_df
        st.session_state.expo_processed = True

        # --- 💡 Summary Section ---
        st.subheader("📊 Expo Processing Summary")

        total_rows = len(final_df)
        sheet_counts = final_df['sheet'].value_counts()
        eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max'])

        st.markdown(f"**✅ Total Rows Processed:** `{total_rows}`")

        cols = st.columns(2)
        with cols[0]:
            st.markdown("**📁 Record Counts by Sheet**")
            st.dataframe(sheet_counts.rename_axis("Sheet").reset_index(name="Records"), use_container_width=True)

        with cols[1]:
            st.markdown("**📅 ETA Date Ranges**")
            eta_stats_display = eta_stats.reset_index()
            st.dataframe(eta_stats_display, use_container_width=True)

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
            elif target_col == "Bond or Non Bond":
                processed_df[target_col] = "Bond"
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
            elif target_col == "Bond or Non Bond":
                processed_df[target_col] = "Non Bond"
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
            elif target_col == "Bond or Non Bond":
                processed_df[target_col] = default_bond_type  # From config
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
            elif target_col == "Bond or Non Bond":
                processed_df[target_col] = pd.NA  # From config
            else:
                processed_df[target_col] = pd.NA

    return processed_df

def show_maersk_summary(final_df):
    st.subheader("📊 Maersk Processing Summary")
    
    total_rows = len(final_df)
    sheet_counts = final_df['sheet'].value_counts()
    eta_stats = final_df.groupby('sheet')['ETA'].agg(['min', 'max']).reset_index()
    
    cols = st.columns(3)
    cols[0].metric("Total Rows", total_rows)
    cols[1].metric("DSR Records", sheet_counts.get('maersk_dsr', 0))
    cols[2].metric("Archived Records", sheet_counts.get('maersk_archived', 0))
    
    st.markdown("**📅 Date Ranges**")
    st.dataframe(eta_stats, hide_index=True, use_container_width=True)


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
                default_bond_type="FCL"
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
        
        # Date Selection with better labels
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
    st.title("📊 DSR Data Processing Pipeline")
    st.caption("Process and combine shipment data from multiple sources")
    
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
            st.subheader("5. Combined Results Overview")
            combined = pd.concat([
                st.session_state.expo_data,
                st.session_state.maersk_data,
                st.session_state.globe_data,
                st.session_state.scanwell_data
            ], ignore_index=True)
            
            st.success(f"✨ Processing complete! Total records: {len(combined):,}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Unique HBLs", combined['HBL'].nunique())
            with col2:
                # Convert ETA to datetime if it's not already
                if 'ETA' in combined.columns:
                    try:
                        combined['ETA'] = pd.to_datetime(combined['ETA'], errors='coerce')
                        valid_dates = combined['ETA'].dropna()
                        if not valid_dates.empty:
                            st.metric("Earliest ETA", valid_dates.min().date())
                        else:
                            st.metric("Earliest ETA", "No valid dates")
                    except Exception as e:
                        st.metric("Earliest ETA", "Date conversion failed")
                else:
                    st.metric("Earliest ETA", "Column not found")

            tab1, tab2 = st.tabs(["📊 Data Preview", "📈 Summary Statistics"])
            
            with tab1:
                st.dataframe(combined.head(), use_container_width=True)
            
            with tab2:
                st.write("**Records by Source:**")
                st.bar_chart(combined['sheet'].value_counts())
                
                # Monthly Shipments chart with proper error handling
                if 'ETA' in combined.columns:
                    try:
                        # Ensure ETA is datetime and drop NaNs
                        combined['ETA'] = pd.to_datetime(combined['ETA'], errors='coerce')
                        monthly_data = combined.dropna(subset=['ETA']).copy()
                        
                        if not monthly_data.empty:
                            monthly_data['Month'] = monthly_data['ETA'].dt.to_period('M')
                            monthly_counts = monthly_data.groupby('Month').size()
                            st.write("**Monthly Shipments:**")
                            st.line_chart(monthly_counts)
                        else:
                            st.warning("No valid dates available for monthly chart")
                    except Exception as e:
                        st.error(f"Could not generate monthly chart: {str(e)}")
                else:
                    st.warning("ETA column not available for monthly chart")  
    # # Final Step: Results
    # elif current_step == 5:
    #     with st.container(border=True):
    #         st.subheader("5. Combined Results Overview")
    #         combined = pd.concat([
    #             st.session_state.expo_data,
    #             st.session_state.maersk_data,
    #             st.session_state.globe_data,
    #             st.session_state.scanwell_data
    #         ], ignore_index=True)
            
    #         st.success(f"✨ Processing complete! Total records: {len(combined):,}")
            
    #         col1, col2 = st.columns(2)
    #         with col1:
    #             st.metric("Unique HBLs", combined['HBL'].nunique())
    #         with col2:
    #                 # With this robust version:
    #                 if not combined.empty and pd.api.types.is_datetime64_any_dtype(combined['ETA']):
    #                     min_date = combined['ETA'].min()
    #                     if pd.notna(min_date):
    #                         st.metric("Earliest ETA", min_date.date())
    #                     else:
    #                         st.metric("Earliest ETA", "No valid dates")
    #                 else:
    #                     st.metric("Earliest ETA", "N/A")            

    #         tab1, tab2 = st.tabs(["📊 Data Preview", "📈 Summary Statistics"])
            
    #         with tab1:
    #             st.dataframe(combined.head(), use_container_width=True)
            
    #         with tab2:
    #             st.write("**Records by Source:**")
    #             st.bar_chart(combined['sheet'].value_counts())
    #             st.write("**Monthly Shipments:**")
    #             st.line_chart(combined.groupby(combined['ETA'].dt.to_period('M')).size())

def main():
    st.set_page_config(
        page_title="DSR Processor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better styling
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