import pandas as pd
# Create list of columns to use
cols = ['zipcode','agi_stub','mars1','MARS2','NUMDEP']
# Create dataframe from csv using only selected columns
data = pd.read_csv("vt_tax_data_2016.csv", usecols=cols)
# View counts of dependents and tax returns by income level
print(data.groupby("agi_stub").sum())


# Create dataframe of next 500 rows with labeled columns
vt_data_next500 = pd.read_csv("vt_tax_data_2016.csv",
                       		  nrows=500,
                       		  skiprows=500,
                       		  header=None,
                       		  names= list(vt_data_first500))
# View the Vermont dataframes to confirm they're different
print(vt_data_first500.head())
print(vt_data_next500.head())



# Create dict specifying data types for agi_stub and zipcode
data_types = {'agi_stub':'category',
			  'zipcode': str}
# Load csv using dtype to set correct data types
data = pd.read_csv("vt_tax_data_2016.csv", dtype=data_types)
# Print data types of resulting frame
print(data.dtypes.head())



# Create dict specifying that 0s in zipcode are NA values
null_values = {'zipcode':0}
# Load csv using na_values keyword argument
data = pd.read_csv("vt_tax_data_2016.csv",
                   na_values=null_values)
# View rows with NA ZIP codes
print(data[data.zipcode.isna()])


try:
	# Import CSV with error_bad_lines set to skip bad records
	data = pd.read_csv("vt_tax_data_2016_corrupt.csv",
					   error_bad_lines=False
					   ,warn_bad_lines=True
					   )
	# View first 5 records
	print(data.head())
except pd.errors.ParserError:
	print("Your data contained rows that could not be parsed.")


# Create string of lettered columns to load
col_string = "AD,AW:BA"
# Load data with skiprows and usecols set
survey_responses = pd.read_excel("fcc_survey_headers.xlsx",
                        skiprows=2,
                        usecols=col_string)
# View the names of the columns selected
print(survey_responses.columns)


# Create df from second worksheet by referencing its name
responses_2017 = pd.read_excel("fcc_survey.xlsx",
                               sheet_name=1)
# Graph where people would like to get a developer job
job_prefs = responses_2017.groupby("JobPref").JobPref.count()
job_prefs.plot.barh()
plt.show()


# Load all sheets in the Excel file
all_survey_data = pd.read_excel("fcc_survey.xlsx",
                                sheet_name=None)
# View the sheet names in all_survey_data
print(all_survey_data.keys())



# Create an empty dataframe
all_responses = pd.DataFrame()
# Set up for loop to iterate through values in responses
for df in responses.values():
  # Print the number of rows being added
  print("Adding {} rows".format(df.shape[0]))
  # Append df to all_responses, assign result
  all_responses = all_responses.append(df)
# Graph employment statuses in sample
counts = all_responses.groupby("EmploymentStatus").EmploymentStatus.count()
counts.plot.barh()
plt.show()


# Load the data
survey_data = pd.read_excel("fcc_survey_subset.xlsx")
# Count NA values in each column
print(survey_data.isna().sum())



# Set dtype to load appropriate column(s) as Boolean data
survey_data = pd.read_excel("fcc_survey_subset.xlsx",
                            dtype={"HasDebt":bool})
# View financial burdens by Boolean group
print(survey_data.groupby('HasDebt').sum())



# Load file with Yes as a True value and No as a False value
survey_subset = pd.read_excel("fcc_survey_yn_data.xlsx",
                              dtype={"HasDebt": bool,
                              "AttendedBootCampYesNo": bool},
                              true_values = ["Yes"],
                              false_values = ["No"])
# View the data
print(survey_subset.head())




# Load file, with Part1StartTime parsed as datetime data
survey_data = pd.read_excel("fcc_survey.xlsx",
                            parse_dates=["Part1StartTime"])
# Print first few values of Part1StartTime
print(survey_data.Part1StartTime.head())



# Create dict of columns to combine into new datetime column
datetime_cols = {"Part2Start": ["Part2StartDate","Part2StartTime"]}
# Load file, supplying the dict to parse_dates
survey_data = pd.read_excel("fcc_survey_dts.xlsx",
                            parse_dates = datetime_cols)
# View summary statistics about Part2Start
print(survey_data.Part2Start.describe())



# Parse datetimes and assign result back to Part2EndTime
survey_data["Part2EndTime"] = pd.to_datetime(survey_data["Part2EndTime"],
                                             format="%m%d%Y %H:%M:%S")
# Print first few values of Part2EndTime
print(survey_data["Part2EndTime"].head())



#-------------SQL----------
# Import sqlalchemy's create_engine() function
from sqlalchemy import create_engine
# Create the database engine
engine = create_engine("sqlite:///data.db")
# View the tables in the database
print(engine.table_names())


# Create the database engine
engine = create_engine("sqlite:///data.db")
# Create a SQL query to load the entire weather table
query = """
SELECT * 
  FROM weather;
"""
# Load weather with the SQL query
weather = pd.read_sql(query, engine)
# View the first few rows of data
print(weather.head())


# Create query to get hpd311calls records about safety
query = """
select *
from hpd311calls
where complaint_type ='SAFETY' ;
"""
# Query the database and assign result to safety_calls
safety_calls = pd.read_sql(query,engine)
# Graph the number of safety calls by borough
call_counts = safety_calls.groupby('borough').unique_key.count()
call_counts.plot.barh()
plt.show()



# Query to join weather to call records by date columns
query = """
SELECT * 
  FROM hpd311calls
  JOIN weather 
  ON hpd311calls.created_date = weather.date;
"""
# Create dataframe of joined tables
calls_with_weather = pd.read_sql(query,engine)
# View the dataframe to make sure all columns were joined
print(calls_with_weather.head())



#----------JSON----------
# Load pandas as pd
import pandas as pd
# Load the daily report to a dataframe
pop_in_shelters = pd.read_json('dhs_daily_report.json')
# View summary stats about pop_in_shelters
print(pop_in_shelters.describe())



try:
	# Load the JSON with orient specified
	df = pd.read_json("dhs_report_reformatted.json",
					  orient='split')
	# Plot total population in shelters over time
	df["date_of_census"] = pd.to_datetime(df["date_of_census"])
	df.plot(x="date_of_census",
			y="total_individuals_in_shelter")
	plt.show()
except ValueError:
	print("pandas could not parse the JSON.")



api_url = "https://api.yelp.com/v3/businesses/search"
# Get data about NYC cafes from the Yelp API
response = requests.get(api_url,
                headers=headers,
                params=params)
# Extract JSON data from the response
data = response.json()
# Load data to a dataframe
cafes = pd.DataFrame(data['businesses'])
# View the data's dtypes
print(cafes.dtypes)