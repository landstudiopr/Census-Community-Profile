import pandas as pd
import numpy as np
from census import Census

# ==========================================
# 1. SETUP
# ==========================================
API_KEY = "00bd97051ecc23de4c25d76e92c060ab7a358b63"
STATE = "72"      # Puerto Rico
COUNTY = "127"    # San Juan
TRACT = "010002"  # Tract 100.02 (Urb. La Cumbre)

YEARS = [2017, 2018, 2019, 2020, 2021, 2022, 2023]

# ==========================================
# 2. FIXED VARIABLE MAPPING
# ==========================================
# Historical Trends
HISTORY_VARS = {
    'B01003_001E': 'Total Population',
    'B01002_001E': 'Median Age',
    'B17001_001E': 'Poverty Universe',
    'B17001_002E': 'People Below Poverty',
    'B19013_001E': 'Median HH Income ($)',
    'B25064_001E': 'Median Gross Rent ($)',
    'B25077_001E': 'Median Home Value ($)'
}

# ==========================================
# 3. FUNCTIONS
# ==========================================

def clean_value(val):
    """Replaces Census error codes (< 0) with None"""
    if val is None: return None
    try:
        f_val = float(val)
        if f_val < 0: return None # Remove -666666666 errors
        return f_val
    except:
        return val

def get_history(c):
    print(f"Fetching 2017-2023 trends (Corrected)...")
    rows = []
    for year in YEARS:
        try:
            data = c.acs5.state_county_tract(fields=list(HISTORY_VARS.keys()), state_fips=STATE, county_fips=COUNTY, tract=TRACT, year=year)
            if data:
                d = data[0]
                
                # Clean variables
                pop = clean_value(d.get('B01003_001E'))
                pov_below = clean_value(d.get('B17001_002E'))
                pov_total = clean_value(d.get('B17001_001E'))
                
                # Calculate Poverty Rate
                pov_rate = 0
                if pov_total and pov_total > 0:
                    pov_rate = round((pov_below / pov_total * 100), 2)

                rows.append({
                    'Year': year,
                    'Total Population': pop,
                    'Median Age': clean_value(d.get('B01002_001E')),
                    'Poverty Rate (%)': pov_rate,
                    'Median HH Income ($)': clean_value(d.get('B19013_001E')),
                    'Median Rent ($)': clean_value(d.get('B25064_001E')),
                    'Median Home Value ($)': clean_value(d.get('B25077_001E'))
                })
        except Exception as e:
            print(f"  - Error {year}: {e}")
            
    return pd.DataFrame(rows)

def get_economic_profile(c):
    """
    Fetches the ENTIRE DP03 (Economic Profile) table for 2023.
    This contains correct Industry, Occupation, Commute, and Employment status data.
    """
    print(f"Fetching 2023 Economic Profile (DP03)...")
    try:
        # Fetch the entire group 'DP03' which contains all economic variables
        data = c.acs5.state_county_tract(fields=['group(DP03)'], state_fips=STATE, county_fips=COUNTY, tract=TRACT, year=2023)
        
        if not data: return pd.DataFrame()
        
        # The result is a single row with hundreds of columns. 
        # We need to reshape it for easier reading in Excel.
        row = data[0]
        
        # Filter for relevant Estimate ('E') columns (skip Percent 'PE' and Margin 'M' if desired, but let's keep E and PE)
        # Note: DP03 variables are labeled DP03_0001E, etc.
        # Since we don't have the labels mapping here, we save the raw data.
        # You will see columns like 'DP03_0001E'. 
        # *** BETTER STRATEGY: ***
        # Since mapping 150 vars is hard, we will stick to the SPECIFIC ONES we know are Industry/Occupation
        # to ensure your CSV is readable.
        
        # MAPPING BASED ON STANDARD 2023 ACS LAYOUT:
        # Industry (Civilian employed 16+)
        # DP03_0033E: Ag, forestry, fishing and hunting, and mining
        # DP03_0034E: Construction
        # DP03_0035E: Manufacturing
        # DP03_0036E: Wholesale trade
        # DP03_0037E: Retail trade
        # DP03_0038E: Transportation and warehousing, and utilities
        # DP03_0039E: Information
        # DP03_0040E: Finance and insurance, and real estate...
        # DP03_0041E: Professional, scientific, and management...
        # DP03_0042E: Educational services, and health care...
        # DP03_0043E: Arts, entertainment, and recreation...
        # DP03_0044E: Other services, except public administration
        # DP03_0045E: Public administration
        
        # Occupation
        # DP03_0026E: Management, business, science, and arts occupations
        # DP03_0027E: Service occupations
        # DP03_0028E: Sales and office occupations
        # DP03_0029E: Natural resources, construction, and maintenance occupations
        # DP03_0030E: Production, transportation, and material moving occupations
        
        target_vars = {
            'DP03_0026E': 'Occ: Mngmt, Business, Science, Arts',
            'DP03_0027E': 'Occ: Service',
            'DP03_0028E': 'Occ: Sales & Office',
            'DP03_0029E': 'Occ: Construction/Maint',
            'DP03_0030E': 'Occ: Production/Transport',
            
            'DP03_0033E': 'Ind: Ag, Forestry, Mining',
            'DP03_0034E': 'Ind: Construction',
            'DP03_0035E': 'Ind: Manufacturing',
            'DP03_0036E': 'Ind: Wholesale Trade',
            'DP03_0037E': 'Ind: Retail Trade',
            'DP03_0038E': 'Ind: Transport, Warehouse, Utilities',
            'DP03_0039E': 'Ind: Information',
            'DP03_0040E': 'Ind: Finance, Ins, Real Estate',
            'DP03_0041E': 'Ind: Prof, Scientific, Mgmt',
            'DP03_0042E': 'Ind: Edu, Health, Social',
            'DP03_0043E': 'Ind: Arts, Entertainment, Food',
            'DP03_0044E': 'Ind: Other Services',
            'DP03_0045E': 'Ind: Public Admin'
        }
        
        # Re-fetch only these specific variables to be safe
        data_clean = c.acs5.state_county_tract(fields=list(target_vars.keys()), state_fips=STATE, county_fips=COUNTY, tract=TRACT, year=2023)
        if not data_clean: return pd.DataFrame()
        
        d = data_clean[0]
        output_rows = []
        for key, label in target_vars.items():
            output_rows.append({
                'Type': 'Occupation' if 'Occ:' in label else 'Industry',
                'Category': label,
                'Count': clean_value(d.get(key))
            })
            
        return pd.DataFrame(output_rows)

    except Exception as e:
        print(f"Error fetching economic profile: {e}")
        return pd.DataFrame()

# ==========================================
# 4. EXECUTION
# ==========================================
def main():
    c = Census(API_KEY)
    
    # 1. History (Corrected)
    df_hist = get_history(c)
    if not df_hist.empty:
        df_hist.to_csv("lacumbre_history_FIXED.csv", index=False)
        print("\n✅ SAVED: lacumbre_history_FIXED.csv (Rent errors removed)")
    
    # 2. Employment (Corrected with DP03)
    df_emp = get_economic_profile(c)
    if not df_emp.empty:
        df_emp.to_csv("lacumbre_employment_2023_FIXED.csv", index=False)
        print("✅ SAVED: lacumbre_employment_2023_FIXED.csv (Correct Industry Data)")
        print(df_emp)

if __name__ == "__main__":
    main()
