import os

import pandas as pd


def get_class_from_google(real: bool = True) -> pd.DataFrame:
    """
    Get the dataframe of reals or bogus from the
    Google Sheets URL stored in the environment variable REALBOGUS_URL.

    :param real: If True, get the real candidates, otherwise get the bogus candidates.
    :return: DataFrame containing the candidates.
    """
    # Extract the sheet ID from the URL
    gid = 0 if real else 1867792539
    sheet_url = os.getenv("REALBOGUS_URL")
    sheet_id = sheet_url.split('/d/')[1].split('/')[0]
    export_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'
    df = pd.read_csv(export_url)
    return df
