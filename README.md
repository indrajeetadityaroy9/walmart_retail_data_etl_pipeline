# Walmart Retail Data Pipeline

Data pipeline for the analysis of supply and demand around the holidays, along with a simple analysis of the data. Two data sources were utilized: `grocery_sales` table in `PostgreSQL` database and a `extra_data` Hadoop parquet fuile with the following features:

### `grocery_sales`
- `index` - unique ID of the row
- `Store_ID` - the store number
- `Date` - the week of sales
- `Weekly_Sales` - sales for the given store
- 
### `extra_data.parquet`
- `IsHoliday` - Whether the week contains a public holiday - 1 if yes, 0 if no.
- `Temperature` - Temperature on the day of sale
- `Fuel_Price` - Cost of fuel in the region
- `CPI` – Prevailing consumer price index
- `Unemployment` - The prevailing unemployment rate
- `MarkDown1`, `"MarkDown2"`, `"MarkDown3"`, `"MarkDown4"` - number of promotional markdowns
- `Dept` - Department Number in each store
- `Size` - size of the store
- `Type` - type of the store (depends on `Size` column)
