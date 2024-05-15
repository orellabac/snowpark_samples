import pandas as pd
import numpy as np

# Create a DataFrame
data = {
    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Emily'],
    'Age': [25, 30, 35, 40, 45],
    'Salary': [50000, 60000, 70000, 80000, 90000],
    'Department': ['HR', 'Finance', 'IT', 'Marketing', 'HR']
}

df = pd.DataFrame(data)

# Display the DataFrame
print("Original DataFrame:")
print(df)
print()

# Basic statistics
print("Basic statistics:")
print(df.describe())
print()

# Calculate mean salary
mean_salary = df['Salary'].mean()
print(f"Mean salary: {mean_salary}")
print()

# Filter data
print("Filtering data where Age is greater than 30:")
filtered_df = df[df['Age'] > 30]
print(filtered_df)
print()

# Group by department and calculate mean age and salary
print("Group by Department and calculate mean Age and Salary:")
grouped_df = df.groupby('Department').agg({'Age': 'mean', 'Salary': 'mean'})
print(grouped_df)
print()

# Add a new column for bonus based on salary
bonus_rate = 0.1
df['Bonus'] = df['Salary'] * bonus_rate
print("DataFrame with Bonus column:")
print(df)
print()

# Sorting by Age
print("Sorting by Age in descending order:")
sorted_df = df.sort_values(by='Age', ascending=False)
print(sorted_df)
print()

# Rename columns
df.rename(columns={'Age': 'Years'}, inplace=True)
print("DataFrame with 'Age' column renamed to 'Years':")
print(df)
print()

# Concatenating two DataFrames
data2 = {
    'Name': ['Frank', 'Grace'],
    'Years': [50, 55],
    'Salary': [100000, 110000],
    'Department': ['Finance', 'IT']
}
df2 = pd.DataFrame(data2)
concatenated_df = pd.concat([df, df2])
print("Concatenated DataFrame:")
print(concatenated_df)
print()

# Reset index
concatenated_df.reset_index(drop=True, inplace=True)
print("DataFrame with reset index:")
print(concatenated_df)
print()

# Save DataFrame to a CSV file
concatenated_df.to_csv('concatenated_data.csv', index=False)
print("DataFrame saved to 'concatenated_data.csv'")
