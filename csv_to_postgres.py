import csv
import psycopg2
from datetime import datetime

def load_csv_to_postgres(csv_file_path, table_name, column_mapping, db_config):
    """
    Load data from a CSV file into a PostgreSQL table.

    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the PostgreSQL table.
    :param column_mapping: Dictionary mapping CSV columns to table columns.
    :param db_config: Dictionary containing database configuration (host, dbname, user, password).
    """
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=db_config['host'],
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = connection.cursor()

        # Open the CSV file
        with open(csv_file_path, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            
            # Print the column names
            print("Columns in CSV:", reader.fieldnames)

            # Retrieve column types for the table
            column_types = get_column_types(connection, table_name)

            for row in reader:
                # Map CSV columns to table columns
                mapped_row = {}
                for table_col, csv_col in column_mapping.items():
                    value = row.get(csv_col, None)
                    print(f"Processing column: {csv_col}, Value: {value}")  # Debugging line
                    if value is None:
                        print(f"Warning: Column '{csv_col}' not found in the CSV file.")
                        continue

                    # Check the column type from the database
                    col_type = column_types.get(table_col, "").lower()
                    if "integer" in col_type or "bigint" in col_type:
                        try:
                            if value == "":  # Handle empty strings for numeric columns
                                value = None  # Set to None (NULL in PostgreSQL)
                            else:
                                value = int(value)  # Convert to integer
                                if abs(value) > 2147483647:  # Check if value exceeds INTEGER range
                                    print(f"Error: Integer value '{value}' out of range for column '{table_col}'. Skipping row.")
                                    continue
                        except ValueError:
                            print(f"Error: Invalid numeric value '{value}' for column '{table_col}'. Skipping row.")
                            continue

                    # Handle date/timestamp conversion if needed
                    if "date" in col_type or "timestamp" in col_type:
                        try:
                            if table_col == "CleanDatetime":  # Specific handling for CleanDatetime
                                value = datetime.strptime(value, '%m/%d/%y %H:%M')
                            else:
                                value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            try:
                                value = datetime.strptime(value, '%m/%d/%y %H:%M')
                            except ValueError as e:
                                print(f"Error processing column '{csv_col}' with value '{value}': {e}")
                                raise

                    mapped_row[table_col] = value

                # Generate SQL query for insertion
                columns = ', '.join(mapped_row.keys())
                placeholders = ', '.join(['%s'] * len(mapped_row))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                # Execute the query
                cursor.execute(sql, list(mapped_row.values()))

        # Commit the transaction
        connection.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_column_types(connection, table_name):
    """
    Retrieve column types for a given table from the PostgreSQL database.

    :param connection: Active database connection.
    :param table_name: Name of the table.
    :return: Dictionary mapping column names to their data types.
    """
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}';
    """
    cursor = connection.cursor()
    cursor.execute(query)
    column_types = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    return column_types

# Example usage
if __name__ == "__main__":
    csv_file_path = "ALICE_UTF8.csv"  # Path to your CSV file
    table_name = "alice_data"  # Name of your PostgreSQL table

    # Define column mapping: CSV column -> Table column
    column_mapping = {
        "date_only":"\ufeffDate_Only",
        "time_only":"Time_Only",
        "originaluserid":"User",
        "user_question":"User_Question",
        "bot_answer":"Bot_Answer",
        "documents_referenced":"Documents_Referenced",
        "general_topic":"General_Topic",
        "user_rating":"User_Rating",
        "user_comments":"User_Comments",
        "userid":"UserID",
        "original":"<-Original",
        "cleandatetime":"CleanDatetime",
        "alice_timestamp":"Timestamp",
        "cleanuserid":"CleanUserID",
        "timediffseconds":"timeDiffSeconds",
        "calculatedconversationid":"calculatedConversationId",
        "cleanreferences":"CleanReferences",
        "cleantopics":"CleanTopics",
        "referencecount":"ReferenceCount",
        "topiccount":"TopicCount"
    }

    # Database configuration
    db_config = {
        "host": "localhost",
        "dbname": "analysis",
        "user": "postgres",
        "password": "masti1234"
    }

    load_csv_to_postgres(csv_file_path, table_name, column_mapping, db_config)
