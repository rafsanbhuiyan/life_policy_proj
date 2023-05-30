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
                if query.lower().startswith('select'):
                    results = cursor.fetchall()
                    return results
                else:
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

    def drop_database(self, database):
        """Drop a database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE {database}")
                self.connection.commit()
                print(f"Database '{database}' dropped successfully")
        except pymysql.Error as e:
            print(f"Error while dropping the database: {repr(e)}")

    def close_connection(self):
        """Closing Connection"""
        if self.connection:
            self.connection.close()
            print("MySQL Connection Closed")

    # Functions for Restful API
    def get_policy_info(self, policy_number):
        """
        Given a policy number, return the effective_date , issue_date ,
        maturity_date , death_benefit , and carrier_name to the best of our knowledge for
        that policy
        """
        try:
            with self.connection.cursor() as cursor:
                sql_query = """
                            SELECT effective_date, issue_date, maturity_date, 
                                   origination_death_benefit as death_benefit, carrier_name  
                            FROM
                            (
                                SELECT *  
                                FROM life_policy_db.policy_normalized AS pn 
                                UNION
                                SELECT *
                                FROM life_policy_db.policy_surplus_records AS psr 
                            ) AS combined
                            WHERE number=%s;
                            """
                cursor.execute(sql_query, (policy_number,))
                result = cursor.fetchone()
                return result
        except Exception as e:
            print(f"Failed to fetch data: {e}")

    def get_carrier_policy_count(self, carrier_name):
        """
        Given a carrier name, returns the count of all unique policies
        we have from that carrier in our database
        """
        try:
            with self.connection.cursor() as cursor:
                sql_query = """
                            SELECT COUNT(number) AS count  
                            FROM
                            (
                                SELECT *  
                                FROM life_policy_db.policy_normalized AS pn 
                                UNION
                                SELECT *
                                FROM life_policy_db.policy_surplus_records AS psr 
                            ) AS combined
                            Where carrier_name=%s;
                            """
                cursor.execute(sql_query, (carrier_name,))
                result = cursor.fetchone()['count']
                return result
        except Exception as e:
            print(f"Failed to fetch data: {e}")

    def get_person_policies(self, person_name):
        """
        Given a person name, return a list of all policies for that
        person regardless the position (primary or secondary) of the person on the
        policy
        """
        try:
            with self.connection.cursor() as cursor:
                sql_query = """
                            SELECT number as policy_number
                            FROM
                            (
                                SELECT ph.*, pn.*
                                FROM life_policy_db.policy_holders AS ph
                                LEFT JOIN life_policy_db.policy_normalized AS pn ON ph.policy_number = pn.number
                                UNION
                                SELECT ph.*, psr.*
                                FROM life_policy_db.policy_holders AS ph
                                LEFT JOIN life_policy_db.policy_surplus_records AS psr ON ph.id = psr.policy_holder_id
                            ) AS combined
                            WHERE primary_name=%s or secondary_name=%s;
                            """
                cursor.execute(sql_query, (person_name, person_name))
                policies = []
                for row in cursor.fetchall():
                    policies.append(row['policy_number'])
                return policies
        except Exception as e:
            print(f"Failed to fetch data: {e}")

    def get_data_provider_policy_count(self, data_provider_code):
        """
        Given a data provider code, return the count of all
        policies that we have information on from that data provider
        """
        try:
            with self.connection.cursor() as cursor:
                sql_query = """
                            SELECT COUNT(number) AS count  
                            FROM
                            (
                                SELECT *  
                                FROM life_policy_db.policy_normalized AS pn 
                                UNION
                                SELECT *
                                FROM life_policy_db.policy_surplus_records AS psr 
                            ) AS combined
                            Where data_provider_code=%s;
                            """
                cursor.execute(sql_query, (data_provider_code,))
                result = cursor.fetchone()['count']
                return result
        except Exception as e:
            print(f"Failed to fetch data: {e}")
