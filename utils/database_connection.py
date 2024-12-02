import pymysql

class DatabaseConnection:
    def __init__(self, host, user, password, database, port=3306):
        """
        Initialize the database connection parameters.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establish the database connection.
        """
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connected to the database successfully!")
        except pymysql.MySQLError as e:
            print(f"Error connecting to the database: {e}")
            raise

    def get_cursor(self):
        """
        Return the cursor for executing queries.
        """
        if self.connection and self.cursor:
            return self.cursor
        else:
            raise Exception("Connection not established. Call connect() first.")

    def commit(self):
        """
        Commit the transaction. Roll back if an error occurs.
        """
        try:
            self.connection.commit()
            print("Transaction committed successfully!")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            print(f"Error during commit. Transaction rolled back. Error: {e}")

    def close(self):
        """
        Close the database connection and cursor.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Database connection closed.")


if __name__ == "__main__":
    db_config = {
        'host': 'srv1376.hstgr.io',
        'user': 'u496382050_ahmad',
        'password': '2009MTayyab$$',
        'database': 'u496382050_stock',
        'port': 3306
    }

    db = DatabaseConnection(**db_config)
    db.connect()
    cursor = db.get_cursor()