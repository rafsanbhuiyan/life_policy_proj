import pymysql.cursors


class MySQLConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = self.connect_to_db()

    def connect_to_db(self):
        """Connect to database"""
        try:
            conn = pymysql.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Connected to MySQL database")
            return conn
        except pymysql.Error as e:
            print(f"Error wile connecting to MySQL DB: {repr(e)}")

    def insert_data(self, table_name, df):
        """convert dataframe into a list of tuple"""
        tuples = [tuple(x) for x in df.to_numpy()]
        # Get columns names from DataFrame
        cols = ','.join(list(df.columns))

        # Formulate the SQL query with placeholders for table name, column names, and values
        # %s are placeholders that will be replaced with actual values when the query is executed
        query = "INSERT INTO %s(%s) VALUES(%s)" % (table_name, cols, ','.join(['%s' for i in range(len(df.columns))]))

        # Create cursor object from connection
        cursor = self.connection.cursor()

        try:
            # Execute the SQL query. The second argument is a list of tuples that you want to insert.
            cursor.executemany(query, tuples)
            # Commit the transaction to the database
            self.connection.commit()
            print(f"Inserted {cursor.rowcount} rows successfully.")
        except Exception as e:
            # If there's an error, print the error
            print("Error: ", e)
        finally:
            # Finally, close the cursor.
            cursor.close()

    def close_connection(self):
        """Closing Connection"""
        if self.connection:
            self.connection.close()
            print("MySQL Connection Closed")
