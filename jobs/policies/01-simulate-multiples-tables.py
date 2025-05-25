"""Script to create 4 tables from 1 file to simulate data from several sources."""

import pandas as pd


def get_s3_location() -> str:
    """Get the S3 location for storing original CSV file.

    Returns:
        str: S3 location.

    """
    return 's3://fcorp-data-prod/raw/policies'


def read_orig_file() -> pd.DataFrame:
    """Get original csv loaded into S3 bucket.

    Returns:
        pd.Dataframe: Original data.

    """
    print('Reading original csv file...')

    return pd.read_csv(f'{get_s3_location()}/orig_file/orig_policies.csv')


def create_policies_types_table(df: pd.DataFrame) -> None:
    """Create policies types table.

    Args:
        df (pd.Dataframe): Dataframe that contains original data.

    Returns: None

    """
    policy_type_dict = {'Corporate Auto': 1,
                        'Personal Auto': 2,
                        'Special Auto': 3}

    output_path = f'{get_s3_location()}/tables/policies_types.csv'

    df['policy_type_id'] = (df['Policy Type']
                            .map(policy_type_dict)
                            .astype(int))

    policies_types_table = (df[['policy_type_id','Policy Type']]
                            .drop_duplicates()
                            .reset_index(drop=True))

    policies_types_table.to_csv(output_path,
                                index=False,
                                header=True,
                                sep=',',
                                encoding='utf-8')
    print(f'policies types table writed successfully in path: {output_path}')

    return None


def create_policy_levels_table(df: pd.DataFrame)-> None:
    """Create policy levels table.

    Args:
        df (pd.Dataframe): Dataframe that contains original data.

    Returns: None

    """
    policy_levels_dict = {'Corporate L3': 1,
                          'Personal L3': 2,
                          'Corporate L2': 3,
                          'Personal L1': 4,
                          'Special L2': 5,
                          'Corporate L1': 6,
                          'Personal L2': 7,
                          'Special L1': 8,
                          'Special L3': 9}

    output_path = f'{get_s3_location()}/tables/policies_levels.csv'


    df['policy_lvl_id'] = (df['Policy']
                           .map(policy_levels_dict)
                           .astype(int))

    policies_levels_table = (df[['policy_lvl_id', 'Policy']]
                             .drop_duplicates()
                             .reset_index(drop=True))

    policies_levels_table.to_csv(output_path,
                                index=False,
                                header=True,
                                sep=',',
                                encoding='utf-8')
    print(f'policies levels table writed successfully in path: {output_path}')

    return None


def create_states_table(df: pd.DataFrame) -> None:
    """Create states table.

    Args:
        df (pd.Dataframe): Dataframe that contains original data.

    Returns: None

    """
    states_dict = {'Washington': 1,
                   'Arizona': 2,
                   'Nevada': 3,
                   'California': 4,
                   'Oregon': 5}

    output_path = f'{get_s3_location()}/tables/states.csv'

    df['state_id'] = (df['State']
                      .map(states_dict)
                      .astype(int))

    states_table = (df[['state_id', 'State']]
                    .drop_duplicates()
                    .reset_index(drop=True))

    states_table.to_csv(output_path,
                                index=False,
                                header=True,
                                sep=',',
                                encoding='utf-8')
    print(f'states table writed successfully in path: {output_path}')

    return None


def create_transactions_table(df: pd.DataFrame) -> None:
    """Create transactions table.

    Args:
        df (pd.Dataframe): Dataframe that contains original data.

    Returns: None

    """
    output_path = f'{get_s3_location()}/tables/transactions.csv'

    transactions_table = df.drop(columns=['Policy Type', 'Policy', 'State'])

    transactions_table.to_csv(output_path,
                                index=False,
                                header=True,
                                sep=',',
                                encoding='utf-8')
    print(f'transactions table writed successfully in path: {output_path}')

    return None


def main() -> None:
    """Create tables to simulate data from several sources."""
    print('Starting tables creation...')

    orig_df = read_orig_file()
    create_policies_types_table(orig_df)
    create_policy_levels_table(orig_df)
    create_states_table(orig_df)
    create_transactions_table(orig_df)

    print('Tables creation succesfully finished.')


if __name__ == '__main__':
    main()
