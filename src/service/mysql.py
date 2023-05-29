import pymysql.cursors


class MySQLConnector:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.connection = self.connect_to_server()

    def connect_to_server(self):
        """Connect to MySQL server"""
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Connected to MySQL server")
            return conn
        except pymysql.Error as e:
            print(f"Error while connecting to MySQL server: {repr(e)}")

    def create_database(self, database):
        """Create a database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {database}")
                self.connection.commit()
                print(f"Database '{database}' created successfully")
        except pymysql.Error as e:
            print(f"Error while creating the database: {repr(e)}")

    def connect_to_db(self, database):
        """Connect to a specific database"""
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=database,
                cursorclass=pymysql.cursors.DictCursor
            )
            print(f"Connected to MySQL database '{database}'")
            return conn
        except pymysql.Error as e:
            print(f"Error while connecting to MySQL DB: {repr(e)}")

    def run_query(self, query):
        """Run an arbitrary SQL query"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                print(f"Query executed successfully")
        except pymysql.Error as e:
            print(f"Error while executing the query: {repr(e)}")

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

    def drop_database(self):
        """Drop the database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE {self.database}")
                print(f"Database '{self.database}' dropped successfully")
        except pymysql.Error as e:
            print(f"Error while dropping the database: {repr(e)}")

    def close_connection(self):
        """Closing Connection"""
        if self.connection:
            self.connection.close()
            print("MySQL Connection Closed")
