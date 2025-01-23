import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Set the style
plt.style.use('seaborn-v0_8-white')
plt.rcParams['font.family'] = 'DejaVu Sans'  # Changed to DejaVu Sans
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

# Create the data
data = {
    'Component': ['HDD\n(1 TB)', 'SSD\n(512 GB)', 'RAM\n(512 GB)', 'CPU'],
    'Emissions_g': [1361.92, 3194.88, 63 * 512, 1924.29],
    'Capacity_GB': [1024, 512, 512, None],
}

# Convert to DataFrame
df = pd.DataFrame(data)
df['Emissions_kg'] = df['Emissions_g'] / 1000
df['Emissions_per_GB'] = np.where(
    df['Capacity_GB'].notna(),
    df['Emissions_g'] / df['Capacity_GB'],
    np.nan
)

# Create figure and subplots with specific dimensions
fig = plt.figure(figsize=(15, 6))

# Create table data
table_data = []
headers = ['Component', 'Capacity', 'Emissions per GB', 'Total Emissions']

for _, row in df.iterrows():
    capacity = f"{row['Capacity_GB']:,.0f} GB" if pd.notna(row['Capacity_GB']) else 'N/A'
    emissions_per_gb = f"{row['Emissions_per_GB']:.2f} g CO₂/GB" if pd.notna(row['Emissions_per_GB']) else 'N/A'
    total_emissions = f"{row['Emissions_kg']:.2f} kg CO₂"

    table_data.append([
        row['Component'].replace('\n', ' '),
        capacity,
        emissions_per_gb,
        total_emissions
    ])

# Left subplot for bar chart
ax1 = plt.subplot(121)

# Create bar plot with enhanced style
bars = ax1.bar(df['Component'], df['Emissions_kg'],
               color='#0ea5e9',  # Deeper blue
               width=0.6,  # Slightly narrower bars
               edgecolor='none')

# Add subtle horizontal grid lines
ax1.yaxis.grid(True, linestyle='--', alpha=0.3)
ax1.set_axisbelow(True)  # Put grid behind bars

# Customize the plot
ax1.set_yscale('log')
ax1.set_ylabel('Total Emissions - Log Scale (kg CO₂)', labelpad=10)
ax1.set_title('Embodied Carbon Emissions\nof Computer Components',
              pad=20, fontsize=14, fontweight='bold')

# Add value labels on top of each bar with enhanced styling
for bar in bars:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width() / 2., height * 1.05,
             f'{height:.2f}',
             ha='center', va='bottom',
             fontsize=10, fontweight='bold')

# Customize y-axis ticks and limits
ax1.set_yticks([1, 2, 5, 10, 20, 50])
ax1.set_ylim(0.9, 50)

# Remove spines
for spine in ['top', 'right']:
    ax1.spines[spine].set_visible(False)

# Thicken remaining spines
for spine in ['left', 'bottom']:
    ax1.spines[spine].set_linewidth(1.5)

# Right subplot for table
ax2 = plt.subplot(122)
ax2.axis('off')

# Create table with enhanced styling
table = ax2.table(
    cellText=table_data,
    colLabels=headers,
    cellLoc='right',
    loc='center',
    bbox=[0.1, 0.2, 0.9, 0.6]  # Adjusted positioning
)

# Enhance table appearance
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1.2, 1.7)

# Style the table cells
for key, cell in table._cells.items():
    cell.set_edgecolor('#dedede')  # Lighter grid lines

    if key[0] == 0:  # Header row
        cell.set_text_props(weight='bold', color='black')
        cell.set_facecolor('#f3f4f6')  # Light gray background
    else:  # Data rows
        cell.set_facecolor('white')

    # Add padding
    cell.PAD = 0.05

# Adjust layout
plt.tight_layout(w_pad=3)  # Increased spacing between subplots

# Show the plot
#plt.show()

# Optionally save the figure with high DPI
# plt.savefig('carbon_emissions.png', dpi=300, bbox_inches='tight', facecolor='white')


# Optionally save the figure
plt.savefig('/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/embodied_carbon_emissions.png', dpi=300, bbox_inches='tight')