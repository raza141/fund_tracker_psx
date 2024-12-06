import pymysql


class DatabaseConnection:
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            if not self.connection or not self.connection.open:
                self.connection = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    port=self.port,
                    connect_timeout=600,  # Increased timeout
                    autocommit=False  # Use autocommit if needed
                )
                print("Connected to the database successfully!")
            self.cursor = AutoReconnectCursor(self.connection)
            return self.cursor
        except pymysql.MySQLError as e:
            print(f"Error connecting to the database: {e}")
            raise

    def commit(self):
        try:
            self._ensure_connection()
            if self.connection:
                self.connection.commit()
                print("Transaction committed successfully!")
        except pymysql.MySQLError as e:
            print(f"Error committing transaction: {e}")
            raise

    def rollback(self):
        try:
            if self.connection:
                self.connection.rollback()
                print("Transaction rolled back successfully!")
        except pymysql.MySQLError as e:
            print(f"Error rolling back transaction: {e}")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Database connection closed.")

    def _ensure_connection(self):
        try:
            if not self.connection.open:
                print("Connection lost. Attempting to reconnect...")
                self.connection.ping(reconnect=True)
        except pymysql.MySQLError as e:
            print(f"Error ensuring connection: {e}")
            raise


class AutoReconnectCursor:
    def __init__(self, connection):
        """
        Initialize the cursor wrapper with auto-reconnect.
        """
        self.connection = connection
        self.cursor = self.connection.cursor()

    def execute(self, query, params=None):
        """
        Execute a query, ensuring the connection is active.
        """
        try:
            self._ensure_connection()
            self.cursor.execute(query, params)
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")
            raise

    def fetchone(self):
        """
        Fetch one row from the result set.
        """
        try:
            return self.cursor.fetchone()
        except pymysql.MySQLError as e:
            print(f"Error fetching one row: {e}")
            raise

    def fetchall(self):
        """
        Fetch all rows from the result set.
        """
        try:
            return self.cursor.fetchall()
        except pymysql.MySQLError as e:
            print(f"Error fetching all rows: {e}")
            raise

    @property
    def lastrowid(self):
        """
        Get the ID of the last inserted row.
        """
        return self.cursor.lastrowid  # Delegate to the native cursor

    def _ensure_connection(self):
        """
        Ensure the database connection is alive and reconnect if necessary.
        """
        try:
            if not self.connection.open:
                print("Connection lost. Reconnecting...")
                self.connection.ping(reconnect=True)
        except pymysql.MySQLError as e:
            print(f"Error ensuring connection: {e}")
            raise

    def close(self):
        """
        Close the cursor.
        """
        self.cursor.close()



if __name__ == "__main__":
    db_config = {
        'host': 'srv1376.hstgr.io',
        'user': 'u496382050_ahmad',
        'password': '2009MTayyab$$',
        'database': 'u496382050_stock',
        'port': 3306
    }

    db = DatabaseConnection(**db_config)
    cursor = db.connect()


