library(RPostgres)
library(DBI)
# Put the credentials in this script
# Never push credentials to git!! --> use .gitignore on .credentials.R
source(".credentials.R")
# Function to send queries to Postgres
source("psql_queries.R")
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg1;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
"create table intg1.Department (
	department_code serial primary key,
	department_name varchar(255),
	department_location varchar(255),
	last_update timestamp(0) without time zone default current_timestamp(0)
);")
# Write rows in the new table
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
"insert into intg1.Department
	values (default, 'Computer Science', 'Aarhus C')
		  ,(default, 'Economics and Business Economics', 'Aarhus V')
		  ,(default, 'Law', 'Aarhus C')
		  ,(default, 'Medicine', 'Aarhus C');")
# Create an R dataframe
df <- data.frame(department_name = c("Education", "Chemistry"),
                 department_location = c("Aarhus N", "Aarhus C"))
# Write the dataframe to a postgres table (columns with default values are skipped)
department <- psql_append_df(cred = cred_psql_docker, 
                             schema_name = "intg1", 
                             tab_name = "department", 
                             df = df)
# Fetching rows into R
psql_select(cred = cred_psql_docker, 
            query_string = "select * from intg1.department;")

# Delete schema
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA intg1 cascade;")

# Exercise 3. From R, do the following in your Postgres server (i.e. the Postgres server running in your postgres container)
# Step 1: Create a New Table in PostgreSQL
# Using the psql_manipulate function to create a table named 'Student' in the 'intg1' schema.
# The table includes 'student_id' as a serial primary key, 'student_name', and 'department_code'.
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table intg1.Student (
                  student_id serial primary key,
                  student_name varchar(255),
                  department_code int);")

# Step 2: Insert Rows into the Table
# Inserting two students, 'Hussein' and 'Johannes', into the 'Student' table with respective department codes.
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "insert into intg1.student
	values (default, 'Hussein', '1')
		  ,(default, 'Johannes', '2');")

# Step 3: Create an R Dataframe
# Creating a dataframe 'df' in R with student names ('Mohammes', 'Adam') and their department codes.
df <- data.frame(student_name = c("Mohammes", "Adam"),
                 department_code = c("1", "2"))

# Step 4: Write the Dataframe to the Postgres Table
# Appending the dataframe 'df' to the 'student' table in the 'intg1' schema of the PostgreSQL database.
# Columns with default values are skipped in this process.
student <- psql_append_df(cred = cred_psql_docker, 
                          schema_name = "intg1", 
                          tab_name = "student", 
                          df = df)

# Step 5: Fetch and Display Rows from the Table into R
# Using the psql_select function to fetch all rows from the 'Student' table and display them in R.
psql_select(cred = cred_psql_docker, 
            query_string = "select * from intg1.student;")



