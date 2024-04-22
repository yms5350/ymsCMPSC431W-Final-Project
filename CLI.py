import psycopg2

def connect_to_db():
    """Connect to the PostgreSQL database server."""
    conn = None
    try:
        # Set your connection parameters here
        conn = psycopg2.connect(
            dbname="postgres",
            user="yatharthshah",
            password="Y27",
            host="localhost"
        )
        return conn
    except Exception as error:
        print(f"Error: {error}")
        return None

# INSERT DATA :   

def insert_student_demographics(conn):
    cursor = conn.cursor()
    print("Inserting into student demographics...")
    student_id = input("Enter student ID: ")
    first_name = input("Enter first name: ")
    last_name = input("Enter last name: ")
    age = input("Enter age: ")
    gender = input("Enter gender: ")
    try:
        cursor.execute("""
            INSERT INTO studentdemographics (student_id, first_name, last_name, age, gender)
            VALUES (%s, %s, %s, %s, %s)
            """, (student_id, first_name, last_name, age, gender))
        conn.commit()
        print("Student demographic data inserted successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def insert_student_academics(conn):
    cursor = conn.cursor()
    print("Inserting into student academics...")
    student_id = input("Enter student ID: ")
    first_name = input("Enter first name: ")
    last_name = input("Enter last name: ")
    major = input("Enter major: ")
    year_in_school = input("Enter year in school: ")
    try:
        cursor.execute("""
            INSERT INTO studentacademics (student_id, first_name, last_name, major, year_in_school)
            VALUES (%s, %s, %s, %s, %s)
            """, (student_id, first_name, last_name, major, year_in_school))
        conn.commit()
        print("Student academic data inserted successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def insert_student_expenses(conn):
    cursor = conn.cursor()
    print("Inserting into student expenses...")
    student_id = input("Enter student ID: ")
    housing = input("Enter housing expenses: ")
    food = input("Enter food expenses: ")
    transportation = input("Enter transportation expenses: ")
    books_supplies = input("Enter books and supplies expenses: ")
    entertainment = input("Enter entertainment expenses: ")
    personal_care = input("Enter personal care expenses: ")
    technology = input("Enter technology expenses: ")
    health_wellness = input("Enter health and wellness expenses: ")
    miscellaneous = input("Enter miscellaneous expenses: ")
    try:
        cursor.execute("""
            INSERT INTO studentexpenses (student_id, housing, food, transportation, books_supplies, entertainment, personal_care, technology, health_wellness, miscellaneous)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (student_id, housing, food, transportation, books_supplies, entertainment, personal_care, technology, health_wellness, miscellaneous))
        conn.commit()
        print("Student expense data inserted successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def insert_student_finances(conn):
    cursor = conn.cursor()
    print("Inserting into student finances...")
    student_id = input("Enter student ID: ")
    monthly_income = input("Enter monthly income: ")
    financial_aid = input("Enter financial aid amount: ")
    tuition = input("Enter tuition amount: ")
    try:
        cursor.execute("""
            INSERT INTO studentfinances (student_id, monthly_income, financial_aid, tuition)
            VALUES (%s, %s, %s, %s)
            """, (student_id, monthly_income, financial_aid, tuition))
        conn.commit()
        print("Student finance data inserted successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def insert_student_payment_methods(conn):
    cursor = conn.cursor()
    print("Inserting into student payment methods...")
    student_id = input("Enter student ID: ")
    preferred_payment_method = input("Enter preferred payment method: ")
    try:
        cursor.execute("""
            INSERT INTO studentpaymentmethods (student_id, preferred_payment_method)
            VALUES (%s, %s)
            """, (student_id, preferred_payment_method))
        conn.commit()
        print("Student payment method data inserted successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def insert_data_menu(conn):
    while True:
        print("\nInsert Data Menu:")
        print("1. Insert Student Demographics")
        print("2. Insert Student Academics")
        print("3. Insert Student Expenses")
        print("4. Insert Student Finances")
        print("5. Insert Student Payment Methods")
        print("0. Go Back")
        choice = input("Enter choice: ")

        if choice == '1':
            insert_student_demographics(conn)
        elif choice == '2':
            insert_student_academics(conn)
        elif choice == '3':
            insert_student_expenses(conn)
        elif choice == '4':
            insert_student_finances(conn)
        elif choice == '5':
            insert_student_payment_methods(conn)
        elif choice == '0':
            break
        else:
            print("Invalid choice. Please try again.")

# UPDATE DATA : 

def update_student_demographics(conn):
    cursor = conn.cursor()
    print("Updating student demographics...")
    student_id = input("Enter student ID to update: ")
    first_name = input("Enter new first name (leave blank if no change): ")
    last_name = input("Enter new last name (leave blank if no change): ")
    age = input("Enter new age (leave blank if no change): ")
    gender = input("Enter new gender (leave blank if no change): ")

    updates = []
    params = []
    if first_name:
        updates.append("first_name = %s")
        params.append(first_name)
    if last_name:
        updates.append("last_name = %s")
        params.append(last_name)
    if age:
        updates.append("age = %s")
        params.append(age)
    if gender:
        updates.append("gender = %s")
        params.append(gender)

    if not updates:
        print("No updates made.")
        return

    query = "UPDATE studentdemographics SET " + ", ".join(updates) + " WHERE student_id = %s;"
    params.append(student_id)

    try:
        cursor.execute(query, params)
        conn.commit()
        print("Student demographic data updated successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def update_student_academics(conn):
    cursor = conn.cursor()
    print("Updating student academics...")
    student_id = input("Enter student ID to update: ")
    major = input("Enter new major (leave blank if no change): ")
    year_in_school = input("Enter new year in school (leave blank if no change): ")

    updates = []
    params = []
    if major:
        updates.append("major = %s")
        params.append(major)
    if year_in_school:
        updates.append("year_in_school = %s")
        params.append(year_in_school)

    if not updates:
        print("No updates made.")
        return

    query = "UPDATE studentacademics SET " + ", ".join(updates) + " WHERE student_id = %s;"
    params.append(student_id)

    try:
        cursor.execute(query, params)
        conn.commit()
        print("Student academics data updated successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def update_student_expenses(conn):
    cursor = conn.cursor()
    print("Updating student expenses...")
    student_id = input("Enter student ID to update: ")
    housing = input("Enter new housing expense (leave blank if no change): ")
    food = input("Enter new food expense (leave blank if no change): ")

    updates = []
    params = []
    if housing:
        updates.append("housing = %s")
        params.append(housing)
    if food:
        updates.append("food = %s")
        params.append(food)

    if not updates:
        print("No updates made.")
        return

    query = "UPDATE studentexpenses SET " + ", ".join(updates) + " WHERE student_id = %s;"
    params.append(student_id)

    try:
        cursor.execute(query, params)
        conn.commit()
        print("Student expenses data updated successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def update_student_finances(conn):
    cursor = conn.cursor()
    print("Updating student finances...")
    student_id = input("Enter student ID to update: ")
    monthly_income = input("Enter new monthly income (leave blank if no change): ")
    financial_aid = input("Enter new financial aid amount (leave blank if no change): ")

    updates = []
    params = []
    if monthly_income:
        updates.append("monthly_income = %s")
        params.append(monthly_income)
    if financial_aid:
        updates.append("financial_aid = %s")
        params.append(financial_aid)

    if not updates:
        print("No updates made.")
        return

    query = "UPDATE studentfinances SET " + ", ".join(updates) + " WHERE student_id = %s;"
    params.append(student_id)

    try:
        cursor.execute(query, params)
        conn.commit()
        print("Student finances data updated successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def update_student_payment_methods(conn):
    cursor = conn.cursor()
    print("Updating student payment methods...")
    student_id = input("Enter student ID to update: ")
    preferred_payment_method = input("Enter new preferred payment method (leave blank if no change): ")

    if not preferred_payment_method:
        print("No updates made.")
        return

    query = "UPDATE studentpaymentmethods SET preferred_payment_method = %s WHERE student_id = %s;"
    params = (preferred_payment_method, student_id)

    try:
        cursor.execute(query, params)
        conn.commit()
        print("Student payment method updated successfully.")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()

def data_menu(conn, action):
    while True:
        print(f"\n{action} Data Menu:")
        print("1. Student Demographics")
        print("2. Student Academics")
        print("3. Student Expenses")
        print("4. Student Finances")
        print("5. Student Payment Methods")
        print("0. Go Back")
        choice = input("Enter choice: ")

        if choice == '1':
            if action == "Insert":
                insert_student_demographics(conn)
            elif action == "Update":
                update_student_demographics(conn)
        elif choice == '2':
            if action == "Insert":
                insert_student_academics(conn)
            elif action == "Update":
                update_student_academics(conn)
        elif choice == '3':
            if action == "Insert":
                insert_student_expenses(conn)
            elif action == "Update":
                update_student_expenses(conn)
        elif choice == '4':
            if action == "Insert":
                insert_student_finances(conn)
            elif action == "Update":
                update_student_finances(conn)
        elif choice == '5':
            if action == "Insert":
                insert_student_payment_methods(conn)
            elif action == "Update":
                update_student_payment_methods(conn)
        elif choice == '0':
            break
        else:
            print("Invalid choice. Please try again.")

# DELETE DATA : 

def delete_student_data(conn):
    student_id = input("Enter student ID to delete all related records, or type 'cancel' to go back: ")

    # Provide an option to cancel the operation
    if student_id.lower() == 'cancel':
        print("Delete operation cancelled.")
        return

    cursor = conn.cursor()

    try:
        # Proceed with the deletion if not cancelled
        print("Proceeding with deletion...")
        # Delete from child tables first to respect foreign key constraints
        cursor.execute("DELETE FROM studentexpenses WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM studentfinances WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM studentpaymentmethods WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM studentacademics WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM studentdemographics WHERE student_id = %s", (student_id,))

        conn.commit()
        print("All data related to student ID", student_id, "has been deleted successfully.")
    except Exception as e:
        print("Failed to delete data:", e)
        conn.rollback()
    finally:
        cursor.close()

# LIST ALL DATA : 

def list_all_student_data(conn):
    print("Listing all student data in a unified view...")
    cursor = conn.cursor()

    try:
        # Execute a SQL query that joins all relevant student tables
        query = """
        SELECT d.student_id, d.first_name, d.last_name, d.age, d.gender,
               a.major, a.year_in_school,
               e.housing, e.food, e.transportation, e.books_supplies, e.entertainment, e.personal_care, e.technology, e.health_wellness, e.miscellaneous,
               f.monthly_income, f.financial_aid, f.tuition,
               p.preferred_payment_method
        FROM studentdemographics d
        LEFT JOIN studentacademics a ON d.student_id = a.student_id
        LEFT JOIN studentexpenses e ON d.student_id = e.student_id
        LEFT JOIN studentfinances f ON d.student_id = f.student_id
        LEFT JOIN studentpaymentmethods p ON d.student_id = p.student_id
        ORDER BY d.student_id;
        """
        cursor.execute(query)
        records = cursor.fetchall()
        
        # Print column headers
        print("\nUnified Student Data:")
        headers = ["Student ID", "First Name", "Last Name", "Age", "Gender", 
                   "Major", "Year in School", 
                   "Housing", "Food", "Transportation", "Books Supplies", "Entertainment", "Personal Care", "Technology", "Health Wellness", "Miscellaneous",
                   "Monthly Income", "Financial Aid", "Tuition",
                   "Preferred Payment Method"]
        print("\t".join(headers))

        # Print each row of the data
        for row in records:
            print("\t".join(str(x) for x in row))

    except Exception as e:
        print("Failed to retrieve data:", e)
    finally:
        cursor.close()

# SEARCH STUDENT DATA : 

def search_student_data(conn):
    student_id = input("Enter student ID to search for: ")
    cursor = conn.cursor()

    try:
        # Execute a SQL query that joins all relevant student tables
        query = """
        SELECT d.student_id, d.first_name, d.last_name, d.age, d.gender,
               a.major, a.year_in_school,
               e.housing, e.food, e.transportation, e.books_supplies, e.entertainment, e.personal_care, e.technology, e.health_wellness, e.miscellaneous,
               f.monthly_income, f.financial_aid, f.tuition,
               p.preferred_payment_method
        FROM studentdemographics d
        LEFT JOIN studentacademics a ON d.student_id = a.student_id
        LEFT JOIN studentexpenses e ON d.student_id = e.student_id
        LEFT JOIN studentfinances f ON d.student_id = f.student_id
        LEFT JOIN studentpaymentmethods p ON d.student_id = p.student_id
        WHERE d.student_id = %s;
        """
        cursor.execute(query, (student_id,))
        record = cursor.fetchone()
        
        # Print the results
        if record:
            headers = ["Student ID", "First Name", "Last Name", "Age", "Gender", 
                       "Major", "Year in School", 
                       "Housing", "Food", "Transportation", "Books Supplies", "Entertainment", "Personal Care", "Technology", "Health Wellness", "Miscellaneous",
                       "Monthly Income", "Financial Aid", "Tuition",
                       "Preferred Payment Method"]
            print("\nDetailed Information for Student ID:", student_id)
            print("\t".join(headers))
            print("\t".join(str(x) for x in record))
        else:
            print("No data found for Student ID:", student_id)

    except Exception as e:
        print("Failed to retrieve data:", e)
    finally:
        cursor.close()


# AGGREGATE STUDENT FINANCES : 

def aggregate_student_finances(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT AVG(monthly_income) AS average_income, SUM(financial_aid) AS total_aid FROM studentfinances")
        result = cursor.fetchone()
        print(f"Average Monthly Income: {result[0]:.2f}")
        print(f"Total Financial Aid: {result[1]:.2f}")
    except Exception as e:
        print("Failed to compute finance aggregates:", e)
    finally:
        cursor.close()

# SORT

def sort_student_demographics(conn):
    cursor = conn.cursor()
    print("Sorting Student Demographics...")
    column = input("Enter column to sort by (e.g., age, gender): ")
    order = input("Choose order (ASC for ascending, DESC for descending): ")
    try:
        cursor.execute(f"SELECT * FROM studentdemographics ORDER BY {column} {order}")
        records = cursor.fetchall()
        for record in records:
            print(record)
    except Exception as e:
        print("Error sorting student demographics:", e)
    finally:
        cursor.close()

def sort_student_academics(conn):
    cursor = conn.cursor()
    print("Sorting Student Academics...")
    column = input("Enter column to sort by (e.g., major, year_in_school): ")
    order = input("Choose order (ASC for ascending, DESC for descending): ")
    try:
        cursor.execute(f"SELECT * FROM studentacademics ORDER BY {column} {order}")
        records = cursor.fetchall()
        for record in records:
            print(record)
    except Exception as e:
        print("Error sorting student academics:", e)
    finally:
        cursor.close()

def sort_student_expenses(conn):
    cursor = conn.cursor()
    print("Sorting Student Expenses...")
    column = input("Enter column to sort by (e.g., housing, food): ")
    order = input("Choose order (ASC for ascending, DESC for descending): ")
    try:
        cursor.execute(f"SELECT * FROM studentexpenses ORDER BY {column} {order}")
        records = cursor.fetchall()
        for record in records:
            print(record)
    except Exception as e:
        print("Error sorting student expenses:", e)
    finally:
        cursor.close()

def sort_student_finances(conn):
    cursor = conn.cursor()
    print("Sorting Student Finances...")
    column = input("Enter column to sort by (e.g., monthly_income, financial_aid): ")
    order = input("Choose order (ASC for ascending, DESC for descending): ")
    try:
        cursor.execute(f"SELECT * FROM studentfinances ORDER BY {column} {order}")
        records = cursor.fetchall()
        for record in records:
            print(record)
    except Exception as e:
        print("Error sorting student finances:", e)
    finally:
        cursor.close()

def sort_student_payment_methods(conn):
    cursor = conn.cursor()
    print("Sorting Student Payment Methods...")
    column = input("Enter column to sort by (e.g., preferred_payment_method): ")
    order = input("Choose order (ASC for ascending, DESC for descending): ")
    try:
        cursor.execute(f"SELECT * FROM studentpaymentmethods ORDER BY {column} {order}")
        records = cursor.fetchall()
        for record in records:
            print(record)
    except Exception as e:
        print("Error sorting student payment methods:", e)
    finally:
        cursor.close()


def sort_data_menu(conn):
    while True:
        print("\nSort Data Menu:")
        print("1. Sort Student Demographics")
        print("2. Sort Student Academics")
        print("3. Sort Student Expenses")
        print("4. Sort Student Finances")
        print("5. Sort Student Payment Methods")
        print("0. Go Back")
        choice = input("Enter choice: ")

        if choice == '1':
            sort_student_demographics(conn)
        elif choice == '2':
            sort_student_academics(conn)
        elif choice == '3':
            sort_student_expenses(conn)
        elif choice == '4':
            sort_student_finances(conn)
        elif choice == '5':
            sort_student_payment_methods(conn)
        elif choice == '0':
            break
        else:
            print("Invalid choice. Please try again.")

# GROUP BY : 

def group_student_demographics_by_gender(conn):
    cursor = conn.cursor()
    print("Grouping Student Demographics by Gender...")
    try:
        cursor.execute("SELECT gender, COUNT(*) FROM studentdemographics GROUP BY gender")
        records = cursor.fetchall()
        for record in records:
            print(f"Gender: {record[0]}, Count: {record[1]}")
    except Exception as e:
        print("Error grouping student demographics by gender:", e)
    finally:
        cursor.close()

def group_student_academics_by_major(conn):
    cursor = conn.cursor()
    print("Grouping Student Academics by Major...")
    try:
        cursor.execute("SELECT major, COUNT(*) FROM studentacademics GROUP BY major")
        records = cursor.fetchall()
        for record in records:
            print(f"Major: {record[0]}, Count: {record[1]}")
    except Exception as e:
        print("Error grouping student academics by major:", e)
    finally:
        cursor.close()

def group_student_expenses_by_category(conn):
    cursor = conn.cursor()
    print("Grouping Student Expenses by Category Total...")
    try:
        cursor.execute("""
        SELECT 'Housing' AS category, SUM(housing) FROM studentexpenses
        UNION ALL
        SELECT 'Food', SUM(food) FROM studentexpenses
        UNION ALL
        SELECT 'Transportation', SUM(transportation) FROM studentexpenses
        """)
        records = cursor.fetchall()
        for record in records:
            print(f"Category: {record[0]}, Total: {record[1]}")
    except Exception as e:
        print("Error grouping student expenses by category:", e)
    finally:
        cursor.close()

def group_student_finances_by_income(conn):
    cursor = conn.cursor()
    print("Grouping Student Finances by Income Range...")
    try:
        cursor.execute("""
        SELECT CASE 
            WHEN monthly_income <= 500 THEN '0-500'
            WHEN monthly_income > 500 AND monthly_income <= 1000 THEN '501-1000'
            ELSE '1001+' END AS income_range,
            COUNT(*) FROM studentfinances
        GROUP BY income_range
        """)
        records = cursor.fetchall()
        for record in records:
            print(f"Income Range: {record[0]}, Count: {record[1]}")
    except Exception as e:
        print("Error grouping student finances by income range:", e)
    finally:
        cursor.close()

def group_student_payment_methods_by_type(conn):
    cursor = conn.cursor()
    print("Grouping Student Payment Methods by Type...")
    try:
        cursor.execute("SELECT preferred_payment_method, COUNT(*) FROM studentpaymentmethods GROUP BY preferred_payment_method")
        records = cursor.fetchall()
        for record in records:
            print(f"Payment Method: {record[0]}, Count: {record[1]}")
    except Exception as e:
        print("Error grouping student payment methods by type:", e)
    finally:
        cursor.close()

def group_data_menu(conn):
    while True:
        print("\nGroup Data Menu:")
        print("1. Group Student Demographics by Gender")
        print("2. Group Student Academics by Major")
        print("3. Group Student Expenses by Category")
        print("4. Group Student Finances by Income Range")
        print("5. Group Student Payment Methods by Type")
        print("0. Go Back")
        choice = input("Enter choice: ")

        if choice == '1':
            group_student_demographics_by_gender(conn)
        elif choice == '2':
            group_student_academics_by_major(conn)
        elif choice == '3':
            group_student_expenses_by_category(conn)
        elif choice == '4':
            group_student_finances_by_income(conn)
        elif choice == '5':
            group_student_payment_methods_by_type(conn)
        elif choice == '0':
            break
        else:
            print("Invalid choice. Please try again.")



# STUDENTS ABOVE AVERAGE : 

def find_students_above_average_expenses(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
        SELECT student_id, housing + food + transportation + books_supplies + entertainment + personal_care + technology + health_wellness + miscellaneous AS total_expenses
        FROM studentexpenses
        WHERE (housing + food + transportation + books_supplies + entertainment + personal_care + technology + health_wellness + miscellaneous) > 
        (SELECT AVG(housing + food + transportation + books_supplies + entertainment + personal_care + technology + health_wellness + miscellaneous) FROM studentexpenses)
        """)
        records = cursor.fetchall()
        for record in records:
            print(f"Student ID: {record[0]}, Total Expenses: {record[1]}")
    except Exception as e:
        print("Error finding students with above-average expenses:", e)
    finally:
        cursor.close()


def main():
    conn = connect_to_db()
    if conn is not None:
        while True:
            print("\nMain Menu:")
            print("1. Insert Data")
            print("2. Update Data")
            print("3. Delete Data")
            print("4. List All Data")
            print("5. Search Data")
            print("6. Sort Data")
            print("7. Group Data")
            print("8. Students Above Average Expenses")
            print("9. Aggregate Student Finances")
            print("0. Exit")
            choice = input("Enter choice: ")

            if choice == '1':
                data_menu(conn, "Insert")
            elif choice == '2':
                data_menu(conn, "Update")
            elif choice == '3':
                delete_student_data(conn)
            elif choice == '4':
                list_all_student_data(conn)
            elif choice == '5':
                search_student_data(conn)
            elif choice == '6':
                sort_data_menu(conn)
            elif choice == '7':
                group_data_menu(conn)
            elif choice == '8':
                find_students_above_average_expenses(conn)
            elif choice == '9':
                aggregate_student_finances(conn)
            elif choice == '0':
                print("Exiting program.")
                break
            else:
                print("Invalid choice. Please try again.")
        conn.close()

if __name__ == '__main__':
    main()

