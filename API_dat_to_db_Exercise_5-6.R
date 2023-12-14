library(RPostgres)
library(DBI)
library(tidyverse)
library(httr2)
library(lubridate)
# Investigate which symbols we can search for ---------------
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("keywords" = "Microsoft",
                "function" = "SYMBOL_SEARCH",
                "datatype" = "json") %>%
  req_headers('X-RapidAPI-Key' = 'b1974985e6msh21f518f4fea290bp189d01jsnbcf11a089d57',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
symbols <- resp %>%
  resp_body_json()
symbols$bestMatches[[1]]
symbols$bestMatches[[2]]

# Extract and Transform  ------------------------------------------
# Extract data from Alpha Vantage
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "1min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "MSFT",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = 'b1974985e6msh21f518f4fea290bp189d01jsnbcf11a089d57',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()

# TRANSFORM timestamp to UTC time
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (1min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")
# Prepare data.frame to hold results
df <- tibble(timestamp = timestamp,
                 open = NA, high = NA, low = NA, close = NA, volume = NA)
# TRANSFORM data into a data.frame
for (i in 1:nrow(df)) {
  df[i,-1] <- as.data.frame(dat$`Time Series (1min)`[[i]])
}

# Create table in Postgres ------------------------------------------------
# Put the credentials in this script
# Never push credentials to git!! --> use .gitignore on .credentials.R
source(".credentials.R")
# Function to send queries to Postgres
source("psql_queries.R")
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg2;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table intg2.prices (
	id serial primary key,
	timestamp timestamp(0) without time zone ,
	open numeric(30,4),
	high numeric(30,4),
	low numeric(30,4),
	close numeric(30,4),
	volume numeric(30,4));")

# LOAD price data -------------------------------
psql_append_df(cred = cred_psql_docker,
               schema_name = "intg2",
               tab_name = "prices",
               df = df)

# Check results -----------------------------------------------------------
# Check that we can fetch the data again
psql_select(cred = cred_psql_docker, 
            query_string = 
              "select * from intg2.prices")
# If you wish, your can delete the schema (all the price data) from Postgres 
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA intg2 cascade;")

# Exercise 5: Using API to Get Data and Transform into DataFrame
# Extract data from Alpha Vantage
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "60min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "AAPL",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = '3cfc543df1msh5f2f9a2573c7694p16bdfcjsnd3efc814e3b9',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()

# Transform the 'timestamp' to UTC time using lubridate package.
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (60min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")

# Exercise 6: Transform Data into an R DataFrame and Load into PostgreSQL
# Prepare an R dataframe 'df' to hold the results with specified columns.
df <- tibble(timestamp = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)

# Transform the fetched data into the dataframe.
for (i in 1:nrow(df)) {
  df[i,-1] <- as.data.frame(dat$`Time Series (60min)`[[i]])
}

# Create Table in PostgreSQL
# Source the credentials from a .credentials.R file (ensure it's in .gitignore for security).
source(".credentials.R")

# Source a function to send queries to Postgres.
source("psql_queries.R")

# Create a new schema 'intg2' in Postgres running in Docker.
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg2;")

# Create a table 'prices' in the new schema with specified columns.
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table intg2.prices (
	id serial primary key,
	timestamp timestamp(0) without time zone ,
	open numeric(30,4),
	high numeric(30,4),
	low numeric(30,4),
	close numeric(30,4),
	volume numeric(30,4));")

# Load Price Data into PostgreSQL
# Append the dataframe 'df' to the 'prices' table in schema 'intg2'.
psql_append_df(cred = cred_psql_docker,
               schema_name = "intg2",
               tab_name = "prices",
               df = df)

# Check Results in PostgreSQL
# Fetch data from the 'prices' table to check if the data is loaded correctly.
psql_select(cred = cred_psql_docker, 
            query_string = "select * from intg2.prices")

# Optional: Delete the Schema from PostgreSQL
# If desired, drop the 'intg2' schema along with all its data from PostgreSQL.
# psql_manipulate(cred = cred_psql_docker, 
#                 query_string = "drop SCHEMA intg2 cascade;")

