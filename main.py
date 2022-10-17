# -*- coding: utf-8 -*-
import mysql.connector
import re


USER = 'root'
PASSWORD = ''
HOST = '127.0.0.1'
DATABASE = 'problem_drinking_dump'
FILE_NAME = '1.sql'#problem_drinking_page.sql
data_r = ['problem_drinking_page', 'problem_drinking_quiz_question', 'problem_drinking_quiz_answer']#'problem_drinking_page', 'problem_drinking_quiz_question', 'problem_drinkingq_uiz_answer', 'problem_drinking_page'


def connect_db(user, password, host, db_name, query='', data=''):
    cnx = mysql.connector.connect(user=user,
                                  password=password,
                                  host=host,
                                  database=db_name)
    if cnx.is_connected():
        cursor = cnx.cursor()
        try:
            if query.strip().upper()[0] == "S":
                cursor.execute(query)
                res = cursor.fetchall()
                return res #list(sum(res, ()))
            else:
                cursor.execute(query, data)
                cnx.commit()
        except Exception as e:
            print("connect_db", e)
        finally:
            cnx.close()


def get_table(line):
    reg = r'INSERT\sINTO\s\`(\w+)\`'
    res = re.findall(reg, line)
    if res:
        return res[0]


def get_cols(line):
    reg = r'INSERT\sINTO\s`\w+`\s\(([\w,`\s]+)\)\sVALUES'
    res = re.findall(reg, line)
    if res:
        return list(filter(lambda score: score != '' and score != ', ', res[0].split('`')))


def get_values(line):
    reg = r'^\((.+)\),?;?$'
    res = re.findall(reg, line)
    if res:
        return eval("["+res[0].strip('"').replace('NULL', "''")+"]")


def get_end_values(line):
    reg = r'^\((.+)\);$'
    res = re.findall(reg, line)
    if res:
        return res[0]


def parse_file(records=[]):
    db_data_s = []
    db_data_e = []
    f_s = True
    with open(FILE_NAME, 'r', encoding="utf-8", newline='') as read_file:
        for i, line in enumerate(read_file):
            if not len(records):
                if f_s:
                    if get_table(line) is not None and get_table(line) == a:
                        db_data_s.append(i)
                        f_s = False
                else:
                    if get_end_values(line) is not None and len(db_data_s):
                        db_data_e.append(i)
                        f_s = True
            else:
                for a in records:
                    if f_s:
                        if get_table(line) is not None and get_table(line) == a:
                            db_data_s.append(i)
                            f_s = False
                    else:
                        if get_end_values(line) is not None and len(db_data_s):
                            db_data_e.append(i)
                            f_s = True
    read_file.close()
    return db_data_s, db_data_e


def write_different_value(table_name, cols_title, data):
    query = f"""
    UPDATE {table_name}
    SET {cols_title}=%s
    WHERE id=%s;
    """
    try:
        connect_db(USER, PASSWORD, HOST, DATABASE, query, (str(data[1]), str(data[0])))
    except Exception as e:
        print("write_different_value", e)


def check_different_value(table_name, cols_title, data):
    query = f"""
    SELECT {cols_title} 
    FROM {table_name}
    WHERE id={data[0]};
    """
    records = connect_db(USER, PASSWORD, HOST, DATABASE, query)
    if records:
        for record in records:
            str1 = data[1] if data[1] else "NULL"
            str2 = record[0]
            if str1.splitlines() != str2.splitlines():
                print("Різні", table_name)
                print(cols_title)
                print(data[0], record[0])
                print(data[0], data[1].strip("'"))
                answer = input("Ви впевнені що хочете змінити дані у колоці? Y - так впевнений!, N - ні не впевнений! ")
                if answer.upper() == 'Y':
                    write_different_value(table_name, cols_title, data)


def parse_current_line(start, end):
    with open(FILE_NAME, 'r', encoding="utf-8", newline='') as read_file:
        content = read_file.readlines()
        p_table_name = get_table(content[start:end][0])
        #print("----------" + p_table_name + "----------")
        flag_cols_position = (search_flag_position(get_cols(content[start:end][0]), "de"))
        for y in flag_cols_position:
            p_cols_name = get_cols(content[start:end][0])[y]
            for x in content[start+1:end+1]:
                check_different_value(p_table_name, p_cols_name, [get_values(x)[0], get_values(x)[y]])


def write_default_value(table_name, cols_title, data):
    for item in data:
        q_d = "NULL" if item[1] is None else str(item[1])
        query = f"""
        UPDATE {table_name}
        SET {cols_title}=%s
        WHERE id=%s;
        """
        try:
            connect_db(USER, PASSWORD, HOST, DATABASE, query, (q_d, str(item[0])))
        except Exception as e:
            print("write_default_value", e)


def select_data_from_cols(table_name, cols_title):
    query = f"""
    SELECT id, {cols_title}
    FROM {table_name}
    """
    try:
        records = connect_db(USER, PASSWORD, HOST, DATABASE, query)
        if records:
            return records
    except Exception as e:
        print("select_data_from_cols", e)


def add_new_table(table_name, cols_title, new_cols_title, datatype):
    nullable = (datatype[1] == 'YES') and 'NULL' or 'NOT NULL'
    query = f"""
    ALTER TABLE {table_name}
    ADD {new_cols_title} {datatype[0]} {nullable} 
    AFTER {cols_title};
    """
    try:
        connect_db(USER, PASSWORD, HOST, DATABASE, query)
    except Exception as e:
        print("add_new_table", e)


def check_cols_datatype(table_name, cols_title):
    query = f"""
    SELECT COLUMN_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE 
        TABLE_NAME   = '{table_name}' 
    AND 
        COLUMN_NAME  = '{cols_title}'
    """
    try:
        records = connect_db(USER, PASSWORD, HOST, DATABASE, query)
        if records:
            return [records[0][0], records[0][1]]
    except Exception as e:
        print("check_cols_datatype", e)


def search_flag_position(search_list, flag):
    return [i for i, x in enumerate(search_list) if x[-3:] == "_"+flag]


def check_add_flags(table_name, cols_title, add_flag):
    res = cols_title.replace("_us", "_" + add_flag)
    if res:
        query = f"""    
        SELECT ORDINAL_POSITION 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE  
            TABLE_NAME='{table_name}' 
        AND 
            COLUMN_NAME='{res}';
        """
        try:
            records = connect_db(USER, PASSWORD, HOST, DATABASE, query)
            if records:
                return [records[0][0], res]
            else:
                return [None, res]
        except Exception as e:
            print("check_add_flags", e)


def check_flags(table_name, cols_title, flag):
    try:
        if cols_title:
            res = cols_title.split("_")
            if res and res.pop() == flag:
                datatype = check_cols_datatype(table_name, cols_title)
                if_exist = check_add_flags(table_name, cols_title, "de")
                if if_exist and if_exist[0] is None:
                    print("----------" + table_name + "----------")
                    print(if_exist[1], datatype)
                    answer = input(
                        "Ви впевнені що хочете добавити нову колонку у таблицю? Y - так впевнений!, N - ні не впевнений! ")
                    if answer.upper() == 'Y':
                        add_new_table(table_name, cols_title, if_exist[1], datatype)
                        data_cols = select_data_from_cols(table_name, cols_title)
                        write_default_value(table_name, if_exist[1], data_cols)
    except Exception as e:
        print("check_flags", e)


def show_cols(table_name):
    query = f"""
    SHOW COLUMNS FROM {table_name};
    """
    try:
        records = connect_db(USER, PASSWORD, HOST, DATABASE, query)
        if records:
            for item in records:
                if item:
                    check_flags(table_name, item[0], "us")
    except Exception as e:
        print("show_cols", e)


def show_tables(records=[]):
    try:
        if not len(records):
            records = connect_db(USER, PASSWORD, HOST, DATABASE, "show tables;")
        for item in records:
            show_cols(item)
    except Exception as e:
        print("show_tables", e)


if __name__ == '__main__':
    show_tables(data_r)
    s_e_list = parse_file(data_r)
    for j, s_line in enumerate(s_e_list[0]):
        parse_current_line(s_e_list[0][j], s_e_list[1][j])
