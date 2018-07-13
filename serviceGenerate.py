__author__ = 'zhangps'

import os
import postgreSQLDB

generate_path = "d:/generate/"
generate_schema = "tobacco"
generate_tables = "video"

# global_interfaces_base_space = "smarthome.service.bases"
# global_model_name_space = "smarthome.service.models"
# global_xml_mapper_name_space = "smarthome.service.xmlmapper"
# global_java_mapper_name_space = "smarthome.service.mappers"
# global_interfaces_name_space = "smarthome.service.interfaces"
# global_services_name_space = "smarthome.service.services"
# global_controller_name_sapce = "smarthome.api.controllers"

global_interfaces_base_space = "smarthome.base.bases"
global_model_name_space = "smarthome.tobacco.models"
global_xml_mapper_name_space = "smarthome.service.xmlmapper"
global_java_mapper_name_space = "smarthome.tobacco.mapper1"
global_interfaces_name_space = "smarthome.tobacco.interfaces"
global_services_name_space = "smarthome.tobacco.services"
global_controller_name_sapce = "smarthome.api.controllers"

'''
global_interfaces_base_space = "lite.iot.api.bases"
global_model_name_space = "lite.iot.api.models"
global_xml_mapper_name_space = "lite.iot.api.xmlmapper"
global_java_mapper_name_space = "lite.iot.api.mappers"
global_interfaces_name_space = "lite.iot.api.interfaces"
global_services_name_space = "lite.iot.api.services"
global_controller_name_sapce = "lite.iot.api.controllers"
'''

# 获取当前数据库下所有表
def get_all_tables():
    conn = postgreSQLDB.get_connection()
    cursor = postgreSQLDB.get_cursor(conn)
    cursor.execute(postgreSQLDB.global_sql_get_db_all_tables)
    table_list = cursor.fetchall()
    postgreSQLDB.close_cursor(cursor)
    postgreSQLDB.close_connection(conn)
    return table_list


# 获取当前数据库下所有视图
def get_all_views():
    conn = postgreSQLDB.get_connection()
    cursor = postgreSQLDB.get_cursor(conn)
    cursor.execute(postgreSQLDB.global_sql_get_db_all_views)
    table_list = cursor.fetchall()
    postgreSQLDB.close_cursor(cursor)
    postgreSQLDB.close_connection(conn)
    return table_list


# 获取Schema和数据库下所有列
def get_all_columns(schema, dbname):
    conn = postgreSQLDB.get_connection()
    cursor = postgreSQLDB.get_cursor(conn)
    cursor.execute(postgreSQLDB.global_sql_get_table_all_clumns, (schema, dbname, schema, dbname))
    columns_list = cursor.fetchall()
    postgreSQLDB.close_cursor(cursor)
    postgreSQLDB.close_connection(conn)
    return columns_list


# sql和java数据类型转换
def get_jdbc_type_from_sql_type(column_type):
    jdbc_type = ""
    jdbc_type = str(column_type).lower()

    if column_type.find("int2") > -1:
        jdbc_type = "INTEGER"
    if column_type.find("smallint") > -1:
        jdbc_type = "INTEGER"
    if column_type.find("int4") > -1:
        jdbc_type = "INTEGER"
    elif column_type.find("int8") > -1:
        jdbc_type = "BIGINT"
    elif column_type.find("bool") > -1:
        jdbc_type = "BIT"
    elif column_type.find("varchar") > -1:
        jdbc_type = "VARCHAR"
    elif column_type.find("text") > -1:
        jdbc_type = "VARCHAR"
    elif column_type.find("date") > -1:
        jdbc_type = "DATE"
    elif column_type.find("timestamp") > -1:
        jdbc_type = "TIMESTAMP"
    elif column_type.find("json") > -1:
        jdbc_type = "OTHER"

    return jdbc_type


# sql和java数据类型转换
def get_java_type_from_sql_type(column_type, return_fullname=False):
    mapping_list = ['', '']
    column_type = str(column_type).lower()
    if column_type.find("int2") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.Integer"
        else:
            mapping_list[0] = "Integer"
    if column_type.find("smallint") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.Integer"
        else:
            mapping_list[0] = "Integer"
    if column_type.find("int4") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.Integer"
        else:
            mapping_list[0] = "Integer"
    elif column_type.find("int8") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.Long"
        else:
            mapping_list[0] = "Long"
    elif column_type.find("bool") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.Boolean"
        else:
            mapping_list[0] = "Boolean"
    elif column_type.find("varchar") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.String"
        else:
            mapping_list[0] = "String"
    elif column_type.find("text") > -1:
        if return_fullname:
            mapping_list[0] = "java.lang.String"
        else:
            mapping_list[0] = "String"
    elif column_type.find("date") > -1:
        if return_fullname:
            mapping_list[0] = "java.util.Date"
        else:
            mapping_list[0] = "Date"
        mapping_list[1] += "import java.util.Date;\r"
    elif column_type.find("timestamp") > -1:
        if return_fullname:
            mapping_list[0] = "java.util.Date"
        else:
            mapping_list[0] = "Date"
        mapping_list[1] += "import java.util.Date;\r"
    elif column_type.find("json") > -1:
        if return_fullname:
            mapping_list[0] = "java.util.Object"
        else:
            mapping_list[0] = "Object"

    return mapping_list


# 从给定的列中找出主键列/非主键列
def get_primary_key_column_from_clumns_list(table_name, columns_list, primary_key=True):
    key_columns = []
    conn = postgreSQLDB.get_connection()
    cursor = postgreSQLDB.get_cursor(conn)
    cursor.execute(postgreSQLDB.global_sql_get_table_primary_key, (table_name,))
    primary_key_column_key = cursor.fetchall()
    postgreSQLDB.close_cursor(cursor)
    postgreSQLDB.close_connection(conn)

    if len(primary_key_column_key) > 0:
        for column in columns_list:
            if primary_key:
                if column[1] == primary_key_column_key[0][0]:
                    key_columns.append(column)
            else:
                if column[1] != primary_key_column_key[0][0]:
                    key_columns.append(column)

    return key_columns


# 根据schema和表名生成单个model
def generate_single_model(schema_name, table_name, is_view=False):
    model_header = model_body =""
    all_columns = get_all_columns(schema_name, table_name)
    model_header = "package {0};\r".format(global_model_name_space)

    model_header += "\rimport java.io.Serializable;\r"

    # 中间部分
    model_body += "\rpublic class {0} implements Serializable {1}\r".format(second_word_behind_capitalize(table_name, "_", True), "{")

    # 添加属性
    for column in get_all_columns(schema_name, table_name):
        column_type = get_java_type_from_sql_type(column[2])[0]
        type_import = get_java_type_from_sql_type(column[2])[1]

        if type_import.strip() != "" and model_header.find(type_import) < 0:
            model_header += type_import

        model_body += "    /**\r"
        model_body += "     * {0}\r".format(column[5])
        model_body += "     */\r"
        model_body += "    protected {0} {1};\r".format(column_type, second_word_behind_capitalize(column[1], "_"))

    model_body += "\r"

    model_body += "    public {0}() {1}\r".format(second_word_behind_capitalize(table_name, "_", True), "{")
    model_body += "    }\r"

    # set constructor
    key_parmas_group = ""
    key_values_group = ""
    if len(all_columns) >= 1:
        column_index = 0
        for column in all_columns:
            if column_index == 0:
                key_parmas_group += "{0} {1}".format(get_java_type_from_sql_type(column[2])[0], second_word_behind_capitalize(column[1], "_"))
                key_values_group += "        this.{0} = {0};\r".format(second_word_behind_capitalize(column[1], "_"))
            else:
                key_parmas_group += ", {0} {1}".format(get_java_type_from_sql_type(column[2])[0], second_word_behind_capitalize(column[1], "_"))
                key_values_group += "        this.{0} = {0};\r".format(second_word_behind_capitalize(column[1], "_"))
            column_index += 1

    model_body += "    public {0}({1}) {2}\r".format(second_word_behind_capitalize(table_name, "_", True), key_parmas_group ,"{")
    model_body += key_values_group
    model_body += "    }\r\r"

    # 添加方法
    for column in get_all_columns(schema_name, table_name):
        column_type = get_java_type_from_sql_type(column[2])[0]

        # get方法
        model_body += "    public {0} get{1}() {2}\r".format(column_type, second_word_behind_capitalize(column[1], "_", True), "{")
        model_body += "        return {0};\r".format(second_word_behind_capitalize(column[1], "_"))
        model_body += "    {0}\r".format("}")

        # set方法
        model_body += "    public void set{0}({1} {2}) {3}\r".format(second_word_behind_capitalize(column[1], "_", True), column_type, second_word_behind_capitalize(column[1], "_"), "{")
        if column_type == "String":
            model_body += "        this.{0} = {0} == null ? null : {0}.trim();\r".format(second_word_behind_capitalize(column[1], "_"))
        else:
            model_body += "        this.{0} = {0};\r".format(second_word_behind_capitalize(column[1], "_"))

        model_body += "    {0}\r".format("}")
        model_body += "\r"

    model_body += "}"

    model_path = os.path.join(generate_path, "api/models")
    if(os.path.exists(model_path) == False):
        os.makedirs(model_path)
    file_single_model = open(os.path.join(model_path, "{0}.java".format(second_word_behind_capitalize(table_name, "_", True))), mode="w", encoding="UTF-8")
    file_single_model.write(model_header + model_body)
    file_single_model.close()


# 根据schema和表名生成单个mybatis_xml_mapper
def generate_single_mybatis_xml_mapper(schema_name, table_name, is_view=False):
    xml_mapper_body = ""
    all_columns = get_all_columns(schema_name, table_name)
    primary_key_column = get_primary_key_column_from_clumns_list(table_name, all_columns)
    common_key_columns = get_primary_key_column_from_clumns_list(table_name, all_columns, False)

    xml_mapper_header = '''<?xml version="1.0" encoding="UTF-8" ?>\r'''
    xml_mapper_header += '''<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd" >\r'''
    xml_mapper_header += '''<mapper namespace="{0}.{1}Mapper" >\r'''.format(global_java_mapper_name_space, second_word_behind_capitalize(table_name, "_", True))

    # result map
    xml_mapper_body += '''  <resultMap id="{0}Map" type="{1}.{0}" >\r'''.format(second_word_behind_capitalize(table_name, "_", True), global_model_name_space)
    xml_mapper_body += "    <constructor >\r"

    if is_view == False:
        for column in primary_key_column:
            xml_mapper_body += '''      <idArg column="{0}" jdbcType="{1}" javaType="{2}" />\r'''.format(column[1], get_jdbc_type_from_sql_type(column[2]), get_java_type_from_sql_type(column[2], True)[0])

        for column in common_key_columns:
            xml_mapper_body += '''      <arg column="{0}" jdbcType="{1}" javaType="{2}" />\r'''.format(column[1], get_jdbc_type_from_sql_type(column[2]), get_java_type_from_sql_type(column[2], True)[0])
    else:
        for column in all_columns:
            xml_mapper_body += '''      <arg column="{0}" jdbcType="{1}" javaType="{2}" />\r'''.format(column[1], get_jdbc_type_from_sql_type(column[2]), get_java_type_from_sql_type(column[2], True)[0])

    xml_mapper_body += "    </constructor>\r"
    xml_mapper_body += "  </resultMap>\r"

    if is_view == False:
        # insert
        xml_mapper_body += '''  <insert id="insert" parameterType="{0}.{1}">\r'''.format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))

        # insert next primary key
        if len(primary_key_column) == 1:
            xml_mapper_body += '''    <selectKey resultType="{0}" keyProperty="{1}" order="BEFORE">\r'''.format(get_java_type_from_sql_type(primary_key_column[0][2], True)[0], second_word_behind_capitalize(primary_key_column[0][1], "_"))
            xml_mapper_body += '''      SELECT {0}\r'''.format(primary_key_column[0][4]).replace("::regclass","")
            xml_mapper_body += "    </selectKey>\r"

        if schema_name == "public":
            xml_mapper_body += '''    insert into "{0}"\r'''.format(table_name)
        else:
            xml_mapper_body += '''    insert into "{0}"."{1}"\r'''.format(schema_name, table_name)

        # insert.columns
        xml_mapper_body += '''    <trim prefix="(" suffix=")" suffixOverrides="," >\r'''
        for column in all_columns:
            if (column[1] == 'update_time'):
                xml_mapper_body += '''      "{0}",\r'''.format(column[1])
            else:
                xml_mapper_body += '''      <if test="{0} != null" >\r'''.format(
                    second_word_behind_capitalize(column[1], "_"))
                xml_mapper_body += '''        "{0}",\r'''.format(column[1])
                xml_mapper_body += "      </if>\r"
        xml_mapper_body+= "    </trim>\r"

        # insert.values
        xml_mapper_body += '''    <trim prefix="values (" suffix=")" suffixOverrides="," >\r'''
        for column in all_columns:
            if (column[1] == 'update_time'):
                xml_mapper_body += '''      CURRENT_TIMESTAMP,\r'''
            else:
                xml_mapper_body += '''      <if test="{0} != null" >\r'''.format(second_word_behind_capitalize(column[1], "_"))
                xml_mapper_body += '''        #{0}{1},jdbcType={2}{3},\r'''.format("{", second_word_behind_capitalize(column[1], "_"), get_jdbc_type_from_sql_type(column[2]), "}")
                xml_mapper_body += "      </if>\r"
        xml_mapper_body += "    </trim>\r"
        xml_mapper_body += "  </insert>\r"

    # count by primary key
    xml_mapper_body += '''  <select id="count" resultType="java.lang.Integer" parameterType="{0}.{1}">\r'''.format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))

    xml_mapper_body += "    select count(*)\r"

    if schema_name == "public":
        xml_mapper_body += '''    from "{0}"\r'''.format(table_name)
    else:
        xml_mapper_body += '''    from "{0}"."{1}"\r'''.format(schema_name, table_name)
    xml_mapper_body += "    <where>\r"

    column_index = 0
    for column in all_columns:
        xml_mapper_body += '''      <if test="{0} != null">\r'''.format(second_word_behind_capitalize(column[1], "_"))
        if column_index == 0:
            xml_mapper_body += '''        "{0}" = #{1}{2}{3}\r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), "}")
        else:
            xml_mapper_body += '''        and "{0}" = #{1}{2}{3}\r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), "}")
        column_index += 1

        xml_mapper_body += "      </if> \r"
    xml_mapper_body += "    </where> \r"

    xml_mapper_body += "  </select>\r"

    if is_view == False:
        # delete by primary key
        xml_mapper_body += '''  <delete id="delete" parameterType="{0}.{1}">\r'''.format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))

        if schema_name == "public":
            xml_mapper_body += '''    delete from "{0}"\r'''.format(table_name)
        else:
            xml_mapper_body += '''    delete from "{0}"."{1}"\r'''.format(schema_name, table_name)
        xml_mapper_body += "    <where>\r"

        column_index = 0
        for column in all_columns:
            xml_mapper_body += '''      <if test="{0} != null"> \r'''.format(second_word_behind_capitalize(column[1], "_"))
            if column_index == 0:
                xml_mapper_body += '''        "{0}" = #{1}{2}{3} \r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), "}")
            else:
                xml_mapper_body += '''        and "{0}" = #{1}{2}{3} \r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), "}")
            column_index += 1

            xml_mapper_body += "      </if> \r"
        xml_mapper_body += "    </where> \r"

        xml_mapper_body += "  </delete>\r"

    if is_view == False:
        # update by primary key
        xml_mapper_body += '''  <update id="updateByPrimaryKey" parameterType="{0}.{1}">\r'''.format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))
        if schema_name == "public":
            xml_mapper_body += '''    update "{0}"\r'''.format(str(table_name))
        else:
            xml_mapper_body += '''    update "{0}"."{1}"\r'''.format(str(schema_name), str(table_name))
        xml_mapper_body += "    <set>\r"

        # update.columns
        for column in common_key_columns:
            if (column[1] == 'update_time'):
                xml_mapper_body += '''      "update_time" = CURRENT_TIMESTAMP,\r'''
            else:
                xml_mapper_body += '''      <if test="{0} != null" >\r'''.format(second_word_behind_capitalize(column[1], "_"))
                xml_mapper_body += '''        "{0}" = #{1}{2},jdbcType={3}{4},\r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), get_jdbc_type_from_sql_type(column[2]), "}")
                xml_mapper_body += "      </if>\r"
        xml_mapper_body += "    </set>\r"

        if len(primary_key_column) == 1:
            xml_mapper_body += '''    where "{0}" = #{1}{2},jdbcType={3}{4}\r'''.format(primary_key_column[0][1],"{", second_word_behind_capitalize(primary_key_column[0][1], "_"), get_jdbc_type_from_sql_type(primary_key_column[0][2]), "}")
        elif len(primary_key_column) > 1:
            xml_mapper_body += "    where"
            column_index = 0
            for column in primary_key_column:
                if column_index == 0:
                    xml_mapper_body += '''"{0}" = #{1}{2},jdbcType={3}{4}'''.format(column[1], "{", column[1], get_jdbc_type_from_sql_type(column[2]), "}")
                else:
                    xml_mapper_body += '''" and {0}" = #{1}{2},jdbcType={3}{4}'''.format(column[1], "{", column[1], get_jdbc_type_from_sql_type(column[2]), "}")
                column_index += 1

        xml_mapper_body += "  </update>\r"

    # select
    xml_mapper_body += '''  <select id="first" resultMap="{1}Map" parameterType="{0}.{1}" >\r'''.format(
        global_model_name_space, second_word_behind_capitalize(table_name, "_", True))
    xml_mapper_body += "    select\r"

    column_index = 0
    order_columns = ''
    for column in all_columns:
        if column_index == 0:
            xml_mapper_body += '''      "{0}", '''.format(column[1])
        elif column_index == len(all_columns) - 1:
            xml_mapper_body += '''"{0}"\r'''.format(column[1])
        else:
            if column_index % 6 == 0:
                xml_mapper_body += "\r      "
                xml_mapper_body += '''"{0}", '''.format(column[1])
            else:
                xml_mapper_body += '''"{0}", '''.format(column[1])
        column_index += 1

    if schema_name == "public":
        xml_mapper_body += '''    from "{0}"\r'''.format(table_name)
    else:
        xml_mapper_body += '''    from "{0}"."{1}"\r'''.format(schema_name, table_name)
    xml_mapper_body += "    <where>\r"

    column_index = 0
    for column in all_columns:
        xml_mapper_body += '''      <if test="{0} != null"> \r'''.format(
            second_word_behind_capitalize(column[1], "_"))
        if column_index == 0:
            xml_mapper_body += '''        "{0}" = #{1}{2}{3} \r'''.format(column[1], "{",
                                                                          second_word_behind_capitalize(column[1],
                                                                                                        "_"), "}")
        else:
            xml_mapper_body += '''        and "{0}" = #{1}{2}{3} \r'''.format(column[1], "{",
                                                                              second_word_behind_capitalize(
                                                                                  column[1], "_"), "}")
        column_index += 1

        xml_mapper_body += "      </if> \r"
    xml_mapper_body += "    </where> \r"

    # order by primary key
    if len(primary_key_column) == 1:
        xml_mapper_body += '''    order by "{0}" limit 1\r'''.format(primary_key_column[0][1])
    elif len(primary_key_column) > 1:
        column_index = 0
        for column in primary_key_column:
            if column_index == 0:
                order_columns += '{0}'.format(column[1])
            else:
                order_columns += ',{0}'.format(column[1])
            column_index += 1

        xml_mapper_body += '''    order by "{0}" limit 1\r'''.format(order_columns)

    xml_mapper_body += "  </select>\r"


    # select
    xml_mapper_body += '''  <select id="select" resultMap="{1}Map" parameterType="{0}.{1}" >\r'''.format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))
    xml_mapper_body += "    select\r"

    column_index = 0
    for column in all_columns:
        if column_index == 0:
            xml_mapper_body += '''      "{0}", '''.format(column[1])
        elif column_index == len(all_columns) - 1:
            xml_mapper_body += '''"{0}"\r'''.format(column[1])
        else:
            if column_index % 6 == 0:
                xml_mapper_body += "\r      "
                xml_mapper_body += '''"{0}", '''.format(column[1])
            else:
                xml_mapper_body += '''"{0}", '''.format(column[1])
        column_index += 1

    if schema_name == "public":
        xml_mapper_body += '''    from "{0}"\r'''.format(table_name)
    else:
        xml_mapper_body += '''    from "{0}"."{1}"\r'''.format(schema_name, table_name)
    xml_mapper_body += "    <where>\r"

    column_index = 0
    for column in all_columns:
        xml_mapper_body += '''      <if test="{0} != null"> \r'''.format(second_word_behind_capitalize(column[1], "_"))
        if column_index == 0:
            xml_mapper_body += '''        "{0}" = #{1}{2}{3} \r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), "}")
        else:
            xml_mapper_body += '''        and "{0}" = #{1}{2}{3} \r'''.format(column[1], "{", second_word_behind_capitalize(column[1], "_"), "}")
        column_index += 1

        xml_mapper_body += "      </if> \r"
    xml_mapper_body += "    </where> \r"

    xml_mapper_body += "  </select>\r"

    xml_mapper_bottom = "</mapper>"

    xml_mapper_path = os.path.join(generate_path, "resources/mappers")
    if (os.path.exists(xml_mapper_path) == False):
        os.makedirs(xml_mapper_path)
    file_single_xml_mapper = open(os.path.join(xml_mapper_path, "{0}Mapper.xml".format(second_word_behind_capitalize(table_name, "_", True))), mode="w", encoding="UTF-8")
    file_single_xml_mapper.write(xml_mapper_header + xml_mapper_body + xml_mapper_bottom)
    file_single_xml_mapper.close()


#根据schema和表名生成单个java_mapper
def generate_single_java_mapper(schema_name, table_name, is_view=False):
    java_mapper_header = java_mapper_body = ""

    java_mapper_header += "package {0};\r\r".format(global_java_mapper_name_space)
    java_mapper_header += "import java.util.List;\r"
    java_mapper_header += "import org.springframework.stereotype.Repository;\r"
    java_mapper_header += "import {0}.{1};\r".format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))
    java_mapper_body += "\r@Repository\r"
    java_mapper_body += "public interface {0}Mapper {1}\r".format(second_word_behind_capitalize(table_name, "_", True), "{")

    if is_view == False:
        java_mapper_body += "    Integer insert({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
    java_mapper_body += "    Integer count({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
    if is_view == False:
        java_mapper_body += "    Integer delete({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
        java_mapper_body += "    Integer updateByPrimaryKey({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
    java_mapper_body += "    {0} first({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
    java_mapper_body += "    List<{0}> select({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
    java_mapper_body += "}\r"

    java_mapper_path = os.path.join(generate_path, "api/mappers")
    if (os.path.exists(java_mapper_path) == False):
        os.makedirs(java_mapper_path)
    java_mapper_interface = open(os.path.join(java_mapper_path, "{0}Mapper.java".format(second_word_behind_capitalize(table_name, "_", True))), mode="w", encoding="UTF-8")
    java_mapper_interface.write(java_mapper_header + java_mapper_body)
    java_mapper_interface.close()


# 根据schema和表名生成单个服务interface
def generate_single_interface(schema_name, table_name, is_view=False):
    interface_header = interface_body = ""
    all_columns = get_all_columns(schema_name, table_name)
    primary_key_column = get_primary_key_column_from_clumns_list(table_name, all_columns)

    interface_header += "package {0};\r".format(global_interfaces_name_space)
    interface_header += "\rimport java.util.List;\r"
    interface_header += "import {0}.{1};\r".format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))
    interface_header += "\rpublic interface I{0}Service {1}\r".format(second_word_behind_capitalize(table_name, "_", True), "{")

    if is_view == False:
        interface_body += "    Integer add({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))

    interface_body += "    Integer count({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))

    if is_view == True:
        primary_key_column = all_columns
    if len(primary_key_column) >= 1:
        primary_key_parmas_group = ""
        column_index = 0
        for column in primary_key_column:
            if column_index == 0:
                primary_key_parmas_group += "{0} {1}".format(get_java_type_from_sql_type(column[2])[0], second_word_behind_capitalize(column[1], "_"))
            else:
                primary_key_parmas_group += ", {0} {1}".format(get_java_type_from_sql_type(column[2])[0], second_word_behind_capitalize(column[1], "_"))
            column_index += 1

        if is_view == False:
            interface_body += "    Integer delete({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
            interface_body += "    Integer update({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
            interface_body += "    Integer addOrUpdate({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
            interface_body += "    {0} getFirst({1});\r".format(second_word_behind_capitalize(table_name, "_", True), primary_key_parmas_group)
        else:
            interface_body += "    {0} getFirst({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
        interface_body += "    List<{0}> getList({0} {1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
        interface_body += "}"

    interface_path = os.path.join(generate_path, "api/interfaces")
    if (os.path.exists(interface_path) == False):
        os.makedirs(interface_path)
    file_interface = open(os.path.join(interface_path, "I{0}Service.java".format(second_word_behind_capitalize(table_name, "_", True))), mode="w", encoding="UTF-8")
    file_interface.write(interface_header + interface_body)
    file_interface.close()


# 根据schema和表名生成单个服务service
def generate_single_service(schema_name, table_name, is_view=False):
    service_header = service_body = ""
    all_columns = get_all_columns(schema_name, table_name)
    primary_key_column = get_primary_key_column_from_clumns_list(table_name, all_columns)

    service_header += "package {0};\r".format(global_services_name_space)

    service_header += "\rimport java.util.List;\r"
    service_header += "import javax.annotation.Resource;\r"
    service_header += "import org.springframework.stereotype.Service;\r"
    service_header += "import {0}.I{1}Service;\r".format(global_interfaces_name_space, second_word_behind_capitalize(table_name, "_", True))
    service_header += "import {0}.{1}Mapper;\r".format(global_java_mapper_name_space, second_word_behind_capitalize(table_name, "_", True))
    service_header += "import {0}.{1};\r".format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))

    service_body += "\r@Service\r"
    service_body += '''public class {0}Service implements I{0}Service {1}\r'''.format(second_word_behind_capitalize(table_name, "_", True), "{")
    service_body += "    @Resource\r"
    service_body += "    private {0}Mapper {1}Mapper;\r\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))

    if is_view == False:
        # add
        service_body += "    @Override\r"
        service_body += "    public Integer add({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "            return 0;\r"
        service_body += "        }\r"
        service_body += "        else{ \r"
        service_body += "            return {0}Mapper.insert({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        service_body += "        }\r"
        service_body += "    }\r"

    service_body += "    @Override\r"
    service_body += "    public Integer count({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
    service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
    service_body += "            return 0;\r"
    service_body += "        }\r"
    service_body += "        else{ \r"
    service_body += "            return {0}Mapper.count({0});\r".format(second_word_behind_capitalize(table_name, "_"))
    service_body += "        }\r"
    service_body += "    }\r"

    if is_view == False:
        # get primary key
        primary_key_parmas_group = ""
        primary_key_set_value_group = ""
        primary_key_get_value_group = ""
        primary_key_parmas_value_not_null_group = ""
        if len(primary_key_column) >= 1:
            column_index = 0
            for column in primary_key_column:
                if column_index == 0:
                    primary_key_parmas_group += "{0} {1}".format(get_java_type_from_sql_type(column[2])[0],  second_word_behind_capitalize(column[1], "_"))
                    primary_key_get_value_group += "{0}.get{1}().toString().isEmpty()".format(second_word_behind_capitalize(table_name, "_"), second_word_behind_capitalize(column[1], "_", True))
                    primary_key_parmas_value_not_null_group += "{0} == null".format(second_word_behind_capitalize(column[1], "_"))
                else:
                    primary_key_parmas_group += ", {0} {1}".format(get_java_type_from_sql_type(column[2])[0], second_word_behind_capitalize(column[1], "_"))
                    primary_key_get_value_group += " && {0}.get{1}().toString().isEmpty()".format(second_word_behind_capitalize(table_name, "_"), second_word_behind_capitalize(column[1], "_", True))
                    primary_key_parmas_value_not_null_group += " ||{0} == null ".format(second_word_behind_capitalize(column[1], "_"))

                primary_key_set_value_group += "            {0}.set{1}({2});\r".format(second_word_behind_capitalize(table_name, "_"), second_word_behind_capitalize(column[1], "_", True), second_word_behind_capitalize(column[1], "_"))
                column_index += 1

        # delete
        if primary_key_parmas_group != "":
            service_body += "    @Override\r"
            service_body += "    public Integer delete({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
            service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
            service_body += "            return 0;\r"
            service_body += "        }\r"
            service_body += "        else{ \r"
            service_body += "            return {0}Mapper.delete({0});\r".format(second_word_behind_capitalize(table_name, "_"))
            service_body += "        }\r"
            service_body += "    }\r"

        # update
        service_body += "    @Override\r"
        service_body += "    public Integer update({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "            return 0;\r"
        service_body += "        }\r"
        service_body += "        else{ \r"
        service_body += "            return {0}Mapper.updateByPrimaryKey({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        service_body += "        }\r"
        service_body += "    }\r"

        # add or update
        service_body += "    @Override\r"
        service_body += "    public Integer addOrUpdate({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "            return 0;\r"
        service_body += "        }\r"
        service_body += "        else{ \r"
        service_body += "            if({0}Mapper.count({0}) == 0){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "                return {0}Mapper.insert({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        service_body += "            }\r"
        service_body += "            else{\r"
        service_body += "                return {0}Mapper.updateByPrimaryKey({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        service_body += "            }\r"
        service_body += "        }\r"
        service_body += "    }\r"

        # get First
        if primary_key_parmas_group != "":
            service_body += "    @Override\r"
            service_body += "    public {0} getFirst({1}) {2}\r".format(second_word_behind_capitalize(table_name, "_", True), primary_key_parmas_group, "{")

            service_body += "        if({0}){1}\r".format(primary_key_parmas_value_not_null_group, "{")
            service_body += "            return null; \r"
            service_body += "        }\r"
            service_body += "        else{\r"
            service_body += "            {0} {1} = new {0}();\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
            service_body += primary_key_set_value_group
            service_body += "            return {0}Mapper.first({0});\r".format(second_word_behind_capitalize(table_name, "_"))
            service_body += "        }\r"
            service_body += "    }\r"

    else:
        # get First
        service_body += "    @Override\r"
        service_body += "    public {0} getFirst({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        service_body += "            return null;\r"
        service_body += "        }\r"
        service_body += "        else{ \r"
        service_body += "            return {0}Mapper.first({0});\r".format(
            second_word_behind_capitalize(table_name, "_"))
        service_body += "        }\r"
        service_body += "    }\r"

    # get List
    service_body += "    @Override\r"
    service_body += "    public List<{0}> getList({0} {1}){2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
    service_body += "        if({0} == null){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
    service_body += "            return null;\r"
    service_body += "        }\r"
    service_body += "        else{ \r"
    service_body += "            return {0}Mapper.select({0});\r".format(second_word_behind_capitalize(table_name, "_"))
    service_body += "        }\r"
    service_body += "    }\r"

    service_body += "}"

    service_path = os.path.join(generate_path, "api/services")
    if (os.path.exists(service_path) == False):
        os.makedirs(service_path)
    file_service = open(os.path.join(service_path, "{0}Service.java".format(second_word_behind_capitalize(table_name, "_", True))), mode="w", encoding="UTF-8")
    file_service.write(service_header + service_body)
    file_service.close()


# 根据schema和表名生成web controller
def generate_single_controller(schema_name, table_name, is_view=False):
    controller_header = controller_body = ""
    all_columns = get_all_columns(schema_name, table_name)
    primary_key_column = get_primary_key_column_from_clumns_list(table_name, all_columns)

    controller_header += "package {0};\r".format(global_controller_name_sapce)
    controller_header += "\rimport java.util.List;\r"
    controller_header += "import javax.annotation.Resource;\r"
    controller_header += "import org.springframework.web.bind.annotation.*;\r"
    controller_header += "import org.springframework.web.servlet.config.annotation.EnableWebMvc;\r"

    controller_header += "import {0}.Code;\r".format(global_interfaces_base_space)
    controller_header += "import {0}.ApiVersion;\r".format(global_interfaces_base_space)
    controller_header += "import {0}.Response;\r".format(global_interfaces_base_space)
    controller_header += "import {0}.{1};\r".format(global_model_name_space, second_word_behind_capitalize(table_name, "_", True))
    controller_header += "import {0}.I{1}Service;\r".format(global_interfaces_name_space, second_word_behind_capitalize(table_name, "_", True))

    controller_body += "\r@RestController\r"
    controller_body += "@EnableWebMvc\r"
    controller_body += "public class {0}Controller {1}\r".format(second_word_behind_capitalize(table_name, "_", True), "{")
    controller_body += "    @Resource\r"
    controller_body += "    private I{0}Service {1}Service;\r\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))

    if is_view == False:
        # insert
        controller_body += "    /**\r"
        controller_body += "     * 新增{0}\r".format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "     * @return 通用Json格式\r"
        controller_body += "     */\r"
        controller_body += '''    @ApiVersion("1.0,latest")\r'''
        controller_body += '''    @RequestMapping(value = "/{0}", method = RequestMethod.POST)\r'''.format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "    public Response add({0} {1}) {2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        controller_body += "        Response rspResult = new Response();\r"
        controller_body += "        if({0}Service.count({0}) == 0){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")

        # get primary key
        primary_keys_added = ""
        if len(primary_key_column) >= 1:
            column_index = 0
            for column in primary_key_column:
                if column_index == 0:
                    primary_keys_added += "{0}Service.add({0})".format(second_word_behind_capitalize(table_name, "_"))
                else:
                    primary_keys_added += " && {0}Service.add({0})".format(second_word_behind_capitalize(table_name, "_"))
                column_index += 1

        controller_body += "            if({0} > 0){1}\r".format(primary_keys_added, "{")
        controller_body += "                rspResult.setData({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "            }\r"
        controller_body += "            else{\r"
        controller_body += '''              rspResult.setCode(Code.INSERT_ERROR);\r'''
        controller_body += "            }\r"
        controller_body += "        }\r"
        controller_body += "        else{\r"
        controller_body += '''            rspResult.setCode(Code.ALREADY_EXISTS);\r'''
        controller_body += "        }\r"
        controller_body += "        return rspResult;\r"
        controller_body += "    }\r"

        # delete
        controller_body += "    /**\r"
        controller_body += "     * 删除{0}\r".format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "     * @return 通用Json格式\r"
        controller_body += "     */\r"
        controller_body += '''    @ApiVersion("1.0,latest")\r'''
        controller_body += '''    @RequestMapping(value = "/{0}", method = RequestMethod.DELETE)\r'''.format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "    public Response delete({0} {1}) {2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        controller_body += "        Response rspResult = new Response();\r"
        controller_body += "        if({0}Service.count({0}) > 0){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        controller_body += "            if({0}Service.delete({0}) > 0){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        controller_body += "                rspResult.setData({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "            }\r"
        controller_body += "            else{\r"
        controller_body += '''              rspResult.setCode(Code.DELETE_ERROR);\r'''
        controller_body += "            }\r"
        controller_body += "        }\r"
        controller_body += "        else{\r"
        controller_body += '''            rspResult.setCode(Code.NOT_EXISTS);\r'''
        controller_body += "        }\r"
        controller_body += "        return rspResult;\r"
        controller_body += "    }\r"

        # update
        controller_body += "    /**\r"
        controller_body += "     * 更新{0}\r".format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "     * @return 通用Json格式\r"
        controller_body += "     */\r"
        controller_body += '''    @ApiVersion("1.0,latest")\r'''
        controller_body += '''    @RequestMapping(value = "/{0}", method = RequestMethod.PUT)\r'''.format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "    public Response update({0} {1}) {2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
        controller_body += "        Response rspResult = new Response();\r"
        controller_body += "        if({0}Service.update({0}) > 0){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
        controller_body += "            rspResult.setData({0});\r".format(second_word_behind_capitalize(table_name, "_"))
        controller_body += "        }\r"
        controller_body += "        else{\r"
        controller_body += '''          rspResult.setCode(Code.UPDATE_ERROR);\r'''
        controller_body += "        }\r"
        controller_body += "        return rspResult;\r"
        controller_body += "    }\r"

    controller_body += "    /**\r"
    controller_body += "     * 获取{0}列表\r".format(second_word_behind_capitalize(table_name, "_"))
    controller_body += "     * @return 通用Json格式\r"
    controller_body += "     */\r"
    controller_body += '''    @ApiVersion("1.0,latest")\r'''
    controller_body += '''    @RequestMapping(value = "/{0}", method = RequestMethod.GET)\r'''.format(second_word_behind_capitalize(table_name, "_"))
    controller_body += "    public Response list({0} {1}) {2}\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"), "{")
    controller_body += "        Response rspResult = new Response();\r"
    controller_body += "        List<{0}> {1}List = {1}Service.getList({1});\r".format(second_word_behind_capitalize(table_name, "_", True), second_word_behind_capitalize(table_name, "_"))
    controller_body += "        if({0}List.size() > 0){1}\r".format(second_word_behind_capitalize(table_name, "_"), "{")
    controller_body += "            rspResult.setData({0}List);\r".format(second_word_behind_capitalize(table_name, "_"))
    controller_body += "        }\r"
    controller_body += "        else{\r"
    controller_body += '''            rspResult.setCode(Code.EMPTY_LIST);\r'''
    controller_body += "        }\r"
    controller_body += "        return rspResult;\r"
    controller_body += "    }\r\r"

    controller_body += "}"

    controller_path = os.path.join(generate_path, "api/controllers")
    if (os.path.exists(controller_path) == False):
        os.makedirs(controller_path)
    file_controller = open(os.path.join(controller_path, "{0}Controller.java".format(second_word_behind_capitalize(table_name, "_", True))), mode="w", encoding="UTF-8")
    file_controller.write(controller_header + controller_body)
    file_controller.close()

# 去掉占位符，并从第二个单词字母大写
def second_word_behind_capitalize(source_string, place_holder, first_word_capitalize=False):
    return_string = source_string.split(place_holder)
    if len(return_string) > 1:
        for index in range(0 if first_word_capitalize else 1, len(return_string)):
            if return_string[index] != '':
                return_string[index] = return_string[index][0].upper() + return_string[index][1:]
            else:
                continue
        return ''.join(return_string)
    else:
        return str(source_string).capitalize() if first_word_capitalize else source_string


for table in get_all_tables():
    schema_name = table[0]
    table_name = table[1]

    if schema_name == generate_schema and generate_tables.lower().find(str(table_name).lower()) > -1:
        # gengreate mode
        generate_single_model(schema_name, table_name)

        # generate xml mapper
        generate_single_mybatis_xml_mapper(schema_name, table_name)

        # generate java mapper
        generate_single_java_mapper(schema_name, table_name)

        # generate interface
        generate_single_interface(schema_name, table_name)

        # generate service
        generate_single_service(schema_name, table_name)

        # generate controller
        generate_single_controller(schema_name, table_name)


for table in get_all_views():
    schema_name = table[0]
    table_name = table[1]

    if schema_name == generate_schema and generate_tables.lower().find(str(table_name).lower()) > -1:
        # gengreate mode
        generate_single_model(schema_name, table_name, True)

        # generate xml mapper
        generate_single_mybatis_xml_mapper(schema_name, table_name, True)

        # generate java mapper
        generate_single_java_mapper(schema_name, table_name, True)

        # generate interface
        generate_single_interface(schema_name, table_name, True)

        # generate service
        generate_single_service(schema_name, table_name, True)

        # generate controller
        generate_single_controller(schema_name, table_name, True)


