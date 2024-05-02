import re
import pandas as pd
import pandas as pd
from tqdm import tqdm
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning


def drop_columns_by_pattern(df, pattern=r"^Unnamed.*"):
    """
    Drops columns from a DataFrame based on a specified pattern.

    Args:
        df (DataFrame): The input DataFrame.
        pattern (str): Regular expression pattern for column matching.

    Returns:
        DataFrame: DataFrame with matching columns dropped.
    """
    # Compile the regex pattern
    regex_pattern = re.compile(pattern, re.IGNORECASE)

    # Find columns matching the pattern
    matching_columns = [col for col in df.columns if regex_pattern.match(col)]

    # Drop matching columns
    df.drop(matching_columns, axis=1, inplace=True)

    try:
        df.drop("index", axis=1, inplace=True)
    except Exception as e:
        print(e)

    return df


def convert_df_to_data_dictionary(
    df, form_name, field_type_map={"*": "text"}, target_output_columns=[""]
):

    # formname == dataframe source name, _take from filename from pat2vec

    # field name is name in df, label is human readable comment

    df.columns = df.columns.str.lower()  # caps throw error

    data_dict = None

    date_files = ["updatetime"]

    output_columns = [
        "Variable / Field Name",
        "Form Name",
        "Section Header",
        "Field Type",
        "Field Label",
        "Choices, Calculations, OR Slider Labels",
        "Field Note",
        "Text Validation Type OR Show Slider Number",
        "Text Validation Min",
        "Text Validation Max",
        "Identifier?",
        "Branching Logic (Show field only if...)",
        "Required Field?",
        "Custom Alignment",
        "Question Number (surveys only)",
        "Matrix Group Name",
        "Matrix Ranking?",
        "Field Annotation",
    ]

    df.columns

    df = drop_columns_by_pattern(df, pattern=r"^Unnamed.*")

    # Add annotation validation columns
    df["human_validation"] = None

    df_columns = df.columns

    print("df_columns", df_columns)

    # data_dict = pd.DataFrame(data=None, columns = df_columns)

    data_rows = []

    for i in range(0, len(df_columns)):

        current_col = df_columns[i]

        # if(current_col in date_files):
        #     field_type = ''

        # else:
        #     field_type = 'text'

        row = {
            "Variable / Field Name": current_col,
            "Form Name": form_name,
            "Field Type": "text",
            "Field Label": current_col,  # use dict lookup and set outside
        }

        data_rows.append(row)

        # data_dict = data_dict.append(row, ignore_index=True)

    data_dict = pd.DataFrame(data=data_rows, columns=output_columns)

    return data_dict


# Example usage: converted dd = convert_df_to_data_dictionary(raw_dump_file, form_name, field_type_map={'*': 'text'}, target_output_columns = [''])
# project.import_metadata(converted_dd, import_format='df')


def import_data(project, raw_dump_file_copy, chunk_size=100):
    # Suppress the warning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    # Calculate the number of chunks
    num_chunks = len(raw_dump_file_copy) // chunk_size + (
        len(raw_dump_file_copy) % chunk_size > 0
    )

    # Create a standalone tqdm progress bar
    progress_bar = tqdm(total=num_chunks, desc="Processing chunks", position=0)

    # Split dataframe into chunks
    chunks = [
        raw_dump_file_copy[i : i + chunk_size]
        for i in range(0, len(raw_dump_file_copy), chunk_size)
    ]

    # Loop through each chunk and make API call
    for i, chunk in enumerate(chunks):
        res = project.import_records(
            to_import=chunk, import_format="df", return_format_type="csv"
        )
        # Process 'res' as needed
        progress_bar.set_description(
            f"Chunk {i+1}/{num_chunks} processed. API returned: {res}"
        )

        # Update the progress bar
        progress_bar.update(1)

    # Close the standalone progress bar
    progress_bar.close()


# Example usage:
# import_data(project, raw_dump_file_copy)


def preprocess_dataframe(dataframe, repeat_instrument_name):
    """
    Preprocesses the given DataFrame by setting 'redcap_repeat_instrument' to 'annotations_epr'
    and setting 'redcap_repeat_instance' starting from 1 as index.

    Parameters:
    dataframe (pd.DataFrame): Input DataFrame to preprocess.

    Returns:
    pd.DataFrame: Preprocessed DataFrame.
    """

    # Make a copy of the DataFrame to avoid modifying the original
    processed_df = dataframe.copy()

    # Set 'redcap_repeat_instrument' to 'annotations_epr'
    processed_df["redcap_repeat_instrument"] = repeat_instrument_name

    # Set instance to index and increment to start from 1
    processed_df.index = range(1, len(processed_df) + 1)

    # Set 'redcap_repeat_instance' as index
    processed_df["redcap_repeat_instance"] = processed_df.index

    return processed_df
