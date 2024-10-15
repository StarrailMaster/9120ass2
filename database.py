#!/usr/bin/env python3
import psycopg2
from numpy.array_api import result_type

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y24s2c9120_xzha0536"
    passwd = "beijixing315"
    myHost = "awsprddbs4836.shared.sydney.edu.au"


    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate staff based on username and password
'''
def checkLogin(login, password):
    try:
        # 连接到 PostgreSQL 数据库
        connection = openConnection()

        # 创建游标对象
        cursor = connection.cursor()

        # 编写 SQL 查询来检查用户名和密码
        query = """
        SELECT UserName, FirstName, LastName, Email
        FROM Administrator
        WHERE UserName = %s AND Password = %s
        """

        # 执行查询并传递参数
        cursor.execute(query, (login.lower(), password))

        # 获取查询结果
        result = cursor.fetchone()

        # 如果查询有结果，返回用户信息
        if result:
            return list(result)  # 将结果转换为列表
        else:
            return None  # 如果用户名或密码不正确，返回 None

    except Exception as error:
        print("Error while connecting to PostgreSQL", error)
        return None

    finally:
        # 确保游标和连接关闭
        if connection:
            cursor.close()
            connection.close()


'''
List all the associated admissions records in the database by staff
'''
def findAdmissionsByAdmin(login):
    conn = openConnection()  # Open the database connection
    cur = conn.cursor()

    try:
        # SQL query to fetch all admissions associated with the administrator and apply sorting
        cur.execute("""
            SELECT AdmissionID as admission_id, 
            AdmissionTypeName as admission_type, 
            DeptName as admission_department,
            TO_CHAR(DischargeDate, 'DD-MM-YYYY') as discharge_date, 
            Fee as fee, 
            FirstName || ' ' || LastName as patient, 
            Condition as condition
            FROM Admission 
                JOIN AdmissionType ON AdmissionType = AdmissionTypeID
			    JOIN Department ON Department = DeptId
			    JOIN Patient ON Patient = PatientID
            WHERE Administrator = %s
            ORDER BY DischargeDate DESC NULLS LAST, FirstName, LastName, AdmissionType DESC; -- 入院类型按降序排列
               """, (login,))

        # Execute query with the admin username (login)
        result = cur.fetchall()
        column1 = ["admission_id"]
        column2 = ["admission_type"]
        column3 = ["admission_department"]
        column4 = ["discharge_date"]
        column5 = ["fee"]
        column6 = ["patient"]
        column7 = ["condition"]

        columns = [dict(zip(column1 + column2 + column3 + column4 + column5 + column6 + column7, row))
                   for row in result]
        # Return the result of the query
        return columns

    except psycopg2.Error as e:
        print("Database error:", e)
        return None

    finally:
        cur.close()
        conn.close()


'''
Find a list of admissions based on the searchString provided as parameter
See assignment description for search specification
'''
def findAdmissionsByCriteria(searchString):
    try:
        conn = openConnection()
        cur = conn.cursor()

        # SQL 查询，使用 ILIKE 实现不区分大小写的匹配
        query = """
            SELECT AdmissionID as admission_id, 
            AdmissionTypeName as admission_type, 
            DeptName as admission_department,
            TO_CHAR(DischargeDate, 'DD-MM-YYYY') as discharge_date, 
            Fee as fee, 
            FirstName || ' ' || LastName as patient, 
            Condition as condition
            FROM Admission 
                JOIN AdmissionType ON AdmissionType = AdmissionTypeID
			    JOIN Department ON Department = DeptId
			    JOIN Patient ON Patient = PatientID
            WHERE (DischargeDate IS NULL OR DischargeDate > (CURRENT_DATE - INTERVAL '2 years'))
                AND
                (LOWER(AdmissionTypeName) LIKE LOWER(%s)
                OR LOWER(DeptName) LIKE LOWER(%s )
                OR LOWER(FirstName || ' ' || LastName) LIKE LOWER(%s)
                OR LOWER(Condition) LIKE LOWER(%s))
            ORDER BY (DischargeDate IS NULL) DESC, FirstName, LastName, DischargeDate ASC; -- 入院类型按降序排列
            """

        # 使用通配符进行关键词匹配
        search_pattern = f'%{searchString}%'
        cur.execute(query, (search_pattern,search_pattern,search_pattern,search_pattern))


        # 将查询结果转换为字典列表
        result = cur.fetchall()
        column1 = ["admission_id"]
        column2 = ["admission_type"]
        column3 = ["admission_department"]
        column4 = ["discharge_date"]
        column5 = ["fee"]
        column6 = ["patient"]
        column7 = ["condition"]

        columns = [dict(zip(column1 + column2 + column3 + column4 + column5 + column6 + column7,
                            ["" if value is None else value for value in row]))
                   for row in result]
        # Return the result of the query
        return columns

    except Exception as error:
        print("Error while connecting to PostgreSQL", error)
        return None

    finally:
        if conn:
            cur.close()
            conn.close()


'''
Add a new addmission 
'''
def addAdmission(type, department, patient, condition, admin):
    
    return


'''
Update an existing admission
'''
def updateAdmission(id, type, department, dischargeDate, fee, patient, condition):
    

    return
