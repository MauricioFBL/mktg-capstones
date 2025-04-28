"""Script to generate simulated marketing campaign datasets for testing purposes."""

import random
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd


def random_date(start: datetime, end: datetime) -> datetime:
    """Generate a random date between two datetime values.

    Args:
        start (datetime): Start date.
        end (datetime): End date.

    Returns:
        datetime: Randomly generated date within the range.

    """
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_campaigns(num_campaigns: int, brands: List[str], types: List[str]) -> pd.DataFrame:
    """Generate a set of simulated marketing campaigns.

    Args:
        num_campaigns (int): Number of campaigns to generate.
        brands (list[str]): List of brand names.
        types (list[str]): List of campaign types.

    Returns:
        pd.DataFrame: DataFrame containing campaign information.

    """
    data = {
        'Campaign_ID': range(1001, 1001 + num_campaigns),
        'Campaign_Name': [
            f'Campaign-{i}-{random.choice(brands)}-{random.choice(types)}'
            for i in range(1, num_campaigns + 1)
        ]
    }
    df = pd.DataFrame(data)
    df.to_csv('./inputs/campaigns.csv', index=False)
    return df


def generate_ad_groups(num_groups: int, campaigns_df: pd.DataFrame) -> pd.DataFrame:
    """Generate a set of simulated ad groups linked to campaigns.

    Args:
        num_groups (int): Number of ad groups to generate.
        campaigns_df (pd.DataFrame): DataFrame of existing campaigns.

    Returns:
        pd.DataFrame: DataFrame containing ad group information.

    """
    data = {
        'Ad_Group_ID': range(2001, 2001 + num_groups),
        'Campaign_ID': np.random.choice(campaigns_df['Campaign_ID'], num_groups),
        'Ad_Group_Name': [
            f'Group-{i}-{np.random.choice(["Women", "Men", "Youth"])}'
            for i in range(1, num_groups + 1)
        ]
    }
    df = pd.DataFrame(data)
    df.to_csv('./inputs/ad_groups.csv', index=False)
    return df


def generate_ads(num_ads: int, ad_groups_df: pd.DataFrame) -> pd.DataFrame:
    """Generate a set of simulated ads linked to ad groups.

    Args:
        num_ads (int): Number of ads to generate.
        ad_groups_df (pd.DataFrame): DataFrame of ad groups.

    Returns:
        pd.DataFrame: DataFrame containing ad information.

    """
    data = {
        'Ad_ID': range(3001, 3001 + num_ads),
        'Ad_Group_ID': np.random.choice(ad_groups_df['Ad_Group_ID'], num_ads),
        'Ad_Name': [
            f'Ad-{i}-{np.random.choice(["Image", "Video", "Carousel"])}'
            for i in range(1, num_ads + 1)
        ],
        'Platform': np.random.choice(['Facebook', 'Instagram', 'Audience Network'], num_ads)
    }
    df = pd.DataFrame(data)
    df.to_csv('./inputs/ads.csv', index=False)
    return df


def generate_daily_data(ads_df: pd.DataFrame, dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Generate simulated daily performance metrics for ads, including platform.

    Args:
        ads_df (pd.DataFrame): DataFrame containing ad information.
        dates (pd.DatetimeIndex): Date range to generate data for.

    Returns:
        pd.DataFrame: DataFrame with daily metrics including Platform.

    """
    daily_data = []

    for date in dates:
        for _, ad_row in ads_df.iterrows():
            ad_id = ad_row['Ad_ID']
            platform = ad_row['Platform']

            impressions = random.randint(100, 5000)
            clicks = random.randint(1, impressions // random.randint(2, 10))
            interactions = random.randint(1, clicks * random.randint(2, 5))
            conversions = random.randint(0, interactions // random.randint(2, 3))

            q25 = random.randint(impressions // 10, impressions // 2)
            q50 = random.randint(q25 // 2, q25)
            q75 = random.randint(q50 // 2, q50)
            completed = random.randint(q75 // 2, q75)

            spend = random.uniform(0.1, 1.0) * (impressions / 1000) + random.uniform(0.05, 0.5) * clicks

            daily_data.append({
                'Date': date.strftime('%Y-%m-%d'),
                'Ad_ID': ad_id,
                'Platform': platform,
                'Impressions': impressions,
                'Clicks': clicks,
                'Interactions': interactions,
                'Conversions': conversions,
                'Quartile_25': q25,
                'Quartile_50': q50,
                'Quartile_75': q75,
                'Completed': completed,
                'Spend': round(spend, 2)
            })

    df = pd.DataFrame(daily_data)
    df.to_csv('./inputs/daily_data.csv', index=False)
    return df


def main() -> None:
    """Generate simulated marketing campaign datasets."""
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)
    dates = pd.date_range(start=start_date, end=end_date)

    campaigns_df = generate_campaigns(
        num_campaigns=5,
        brands=['Brand A', 'Brand B', 'Brand C'],
        types=['Promotion', 'Awareness', 'Launch']
    )

    ad_groups_df = generate_ad_groups(num_groups=15, campaigns_df=campaigns_df)
    ads_df = generate_ads(num_ads=50, ad_groups_df=ad_groups_df)
    generate_daily_data(ads_df=ads_df, dates=dates)

    print("✅ CSV files successfully generated:")
    print(" - campaigns.csv")
    print(" - ad_groups.csv")
    print(" - ads.csv")
    print(" - daily_data.csv")


if __name__ == '__main__':
    main()
