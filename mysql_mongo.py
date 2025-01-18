import psycopg2

# PostgreSQL connection parameters
database = 'revoke'
username = 'postgres'
password = 'kalijati123'
host = 'localhost'
sql_file = 'ops.sql'

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=database,
    user=username,
    password=password,
    host=host
)

# Create cursor
cur = conn.cursor()

# Execute SQL file
with open(sql_file, 'r') as file:
    sql = file.read()
    cur.execute(sql)

# Commit changes
conn.commit()

# Close connection
conn.close()