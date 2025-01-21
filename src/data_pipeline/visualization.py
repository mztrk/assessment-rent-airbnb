import matplotlib.pyplot as plt
import pandas as pd


def generate_visualizations(airbnb_df, output_dir):
    """
    Generates visualizations for insights.

    Args:
        airbnb_df (pyspark.sql.DataFrame): Transformed Airbnb DataFrame.
        output_dir (str): Directory to save visualizations.
    """
    # Convert to Pandas for visualization
    airbnb_pd = airbnb_df.toPandas()

    # Average price by postcode
    avg_price = airbnb_pd.groupby("cleaned_zipcode")["price"].mean().sort_values()
    plt.figure(figsize=(10, 6))
    avg_price.plot(kind="bar", color="skyblue")
    plt.title("Average Price by Postcode")
    plt.xlabel("Postcode")
    plt.ylabel("Average Price (€)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/average_price_by_postcode.png")

    # Room type distribution
    room_type_counts = airbnb_pd["room_type"].value_counts()
    plt.figure(figsize=(8, 5))
    room_type_counts.plot(
        kind="pie", autopct="%1.1f%%", colors=["#ff9999", "#66b3ff", "#99ff99"]
    )
    plt.title("Room Type Distribution")
    plt.ylabel("")  # Hide y-axis label
    plt.savefig(f"{output_dir}/room_type_distribution.png")


def generate_investment_visualizations(investment_metrics, output_dir):
    """
    Generates visualizations for investment analysis.
    """
    investment_pd = investment_metrics.toPandas()

    # Bar chart for revenue by investment type
    revenue_by_type = investment_pd.groupby("investment_type")[
        ["avg_airbnb_revenue", "avg_rental_revenue"]
    ].mean()
    revenue_by_type.plot(kind="bar", figsize=(10, 6), color=["skyblue", "salmon"])
    plt.title("Average Revenue by Investment Type")
    plt.ylabel("Revenue (€)")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/revenue_by_investment_type.png")
