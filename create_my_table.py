import easydbs

# Create connection and cursor.
sqlite = easydbs.connect(db_type='sqlite', db_name='app.db')
cursor = sqlite.cursor()

# Create table my_table.
cursor.execute("CREATE TABLE IF NOT EXISTS my_table (id INT, name STRING)")

# Inserte rows.
rows = [(1, 'ian'), (2, 'pierre'), (3, 'jacques')]
cursor.executemany("INSERT INTO my_table VALUES (?, ?)", rows)

# Read tables.
cursor.execute("SELECT * FROM my_table")
for row in cursor.fetchall():
    print(row)


