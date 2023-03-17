import matplotlib.pyplot as plt
import locale
import os

def visualize(df):
   
    dir_path = "output"

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    # Group the data by recommended mode and calculate the mean current and recommended costs
    mode_costs = df.groupby('recommended_mode')[
        ['current_cost', 'recommended_cost']].sum()

    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

    mode_costs = mode_costs.applymap(
        lambda x: locale.currency(x, grouping=True))

    mode_costs = mode_costs.applymap(lambda x: ''.join(
        c for c in x if c.isdigit() or c == '.'))

    mode_costs = mode_costs.applymap(lambda x: float(x))
    ax = mode_costs.plot(figsize=(18, 11), kind='bar',
                         color=['orange', 'green'])

    # Add labels and title
    plt.xlabel('Recommended Mode')
    plt.ylabel('Cost')
    plt.title('Current and Recommended Costs by Recommended Mode')

    # Add values on top of each bar
    for i, (name, row) in enumerate(mode_costs.iterrows()):
        ax.text(i - 0.20, row['current_cost'] + 1, locale.currency(
            row['current_cost'], grouping=True), color='black', fontweight='bold')
        ax.text(i + 0.05, row['recommended_cost'] + 1, locale.currency(
            row['recommended_cost'], grouping=True), color='black', fontweight='bold')

    # Add note with count of unique tables, accounts, regions, and number of days
    unique_tables = len(df['base_table_name'].unique())

    
    note = f"Data includes {unique_tables} tables"
    ax.set_title("\n" + note, color='orange', fontsize=15)

    # Display the chart
    ax.figure.savefig(os.path.join(dir_path, 'recommendation_chart.png'))