from pyspark.sql.functions import col, count, when, approx_count_distinct

from extract import name_basics_df


def analyze_and_drop_uninformative_features(df, null_threshold=0.8, unique_threshold=1):
    """
    Analyzes feature informativeness and drops uninformative ones:
    - If the percentage of missing values (null) > null_threshold
    - If the number of unique values <= unique_threshold (e.g., constant value)
    """
    total_rows = df.count()
    if total_rows == 0:
        return df

    # Get the number of null values for each column
    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    null_counts = df.select(*null_exprs).collect()[0].asDict()

    # Get the approximate number of unique values for each column
    unique_exprs = [approx_count_distinct(col(c)).alias(c) for c in df.columns]
    unique_counts = df.select(*unique_exprs).collect()[0].asDict()

    columns_to_drop = []
    print("\n--- 1. Feature Informativeness Analysis ---")
    for c in df.columns:
        null_pct = null_counts[c] / total_rows
        unique_vals = unique_counts[c]

        print(f"Feature '{c}': {null_pct*100:.2f}% missing, ~{unique_vals} unique values")

        if null_pct > null_threshold:
            print(f"  -> Dropping '{c}' (too many missing values: {null_pct*100:.2f}% > {null_threshold*100}%)")
            columns_to_drop.append(c)
        elif unique_vals <= unique_threshold:
            print(f"  -> Dropping '{c}' (not enough unique values: {unique_vals} <= {unique_threshold})")
            columns_to_drop.append(c)

    print("-------------------------------------------\n")

    # Drop the collected columns
    if columns_to_drop:
        df = df.drop(*columns_to_drop)

    return df

def analyze_and_handle_duplicates(df):
    """
    Analyzes and removes duplicate rows from the DataFrame.
    """
    total_rows = df.count()
    df_cleaned = df.dropDuplicates()
    cleaned_rows = df_cleaned.count()
    duplicates_count = total_rows - cleaned_rows
    
    print("\n--- 2. Duplicates Analysis ---")
    print(f"Total rows before: {total_rows}")
    print(f"Duplicates found and removed: {duplicates_count}")
    print(f"Total rows after: {cleaned_rows}")
    print("------------------------------\n")
    
    return df_cleaned

def analyze_and_handle_missing_values(df, strategy="drop"):
    """
    Analyzes missing values in remaining rows and handles them.
    Available strategies: 
    - 'drop': drops any row containing at least one null value
    - 'fill': fills nulls with default values ('Unknown' for strings, 0 for numbers)
    """
    print(f"\n--- 3. Missing Values Row Analysis (Strategy: {strategy}) ---")
    total_rows = df.count()
    
    # Calculate rows with at least one null value
    df_dropped = df.dropna()
    dropped_rows_count = total_rows - df_dropped.count()
    
    print(f"Rows with at least one missing value: {dropped_rows_count} ({(dropped_rows_count/total_rows)*100:.2f}%)")
    
    if strategy == "drop":
        print("Action: Dropped rows with missing values.")
        df_final = df_dropped
    elif strategy == "fill":
        print("Action: Filled missing values with defaults ('Unknown' / 0).")
        # Fill strings with 'Unknown' and numbers with 0
        df_final = df.fillna("Unknown").fillna(0)
    else:
        df_final = df
        
    print("------------------------------------------------------\n")
    return df_final

if __name__ == "__main__":
    print("=== DATA CLEANING PIPELINE ===")
    print("Running analysis on the 'name_basics_df' dataset as an example.")
    
    # To speed up computation for the example, we take a sample of 100,000 rows
    sample_df = name_basics_df.limit(100000)
    
    # Step 1: Remove uninformative columns (e.g. columns with >50% nulls)
    df_step1 = analyze_and_drop_uninformative_features(sample_df, null_threshold=0.5, unique_threshold=1)
    
    # Step 2: Remove duplicated rows
    df_step2 = analyze_and_handle_duplicates(df_step1)
    
    # Step 3: Handle remaining missing values (dropping rows with any missing values)
    # We use strategy="drop" but you can also use "fill" to preserve rows
    df_cleaned = analyze_and_handle_missing_values(df_step2, strategy="drop")
    
    print("Data after full cleaning pipeline:")
    df_cleaned.show(5)
