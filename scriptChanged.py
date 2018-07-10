scriptFile = open("D:\\DataBase\\Script\\tobacco.sql", "r")

text = ""
drop_sequence= ""
create_sequence= ""
schemaname=""
tablename=""
drop_view=""
drop_table=""
while True:
    line = scriptFile.readline()
    if line:
        if line.find("_id") > -1:
            line = line.replace("INT4", "BIGINT").replace("INT8", "BIGINT").replace("integer", "BIGINT")
        if line.find("_account") > -1:
            line = line.replace("INT4", "BIGINT").replace("INT8", "BIGINT").replace("integer", "BIGINT")
        if line.find("INT8") > -1:
            line = line.replace("INT8", "BIGINT")
        if line.find("=========") > -1:
            pass
        elif line.find("DBMS name:") > -1:
            pass
        elif line.find("Created on:") > -1:
            pass
        elif line.find("set table ownership") > -1:
            continue
        elif line.find("set view ownership") > -1:
            continue
        elif line.find("alter table") > -1 and line.find("owner to") > -1:
            continue
        if line.find("drop view") > -1:
            drop_view += line
        if line.find("drop table") > -1:
            drop_table += line
        elif line.find(" DATE ") > -1:
            text += line.replace(" DATE ", "timestamp").replace("CURRENT_DATE", "CURRENT_TIMESTAMP")
        elif line.find("create table ") > -1:
            tablename = line[line.find("create table ") + 13:line.find(" (")]
            if tablename.find(".") > -1:
                schemaname = tablename.split(".")[0]
            text += line
        elif line.find("SERIAL") > -1:
            array = line.replace(" ","").split("SERIAL")
            primary_key = array[0]
            ext_primary_key = array[0]
            if primary_key == "data_id":
                ext_primary_key = tablename.replace("data", "") + primary_key
            elif primary_key == "extend_id":
                ext_primary_key = tablename.replace("extend", "") + primary_key

            if schemaname == "":
                text += "   {0}    BIGINT default nextval('seq_{1}')    {2}".format(array[0], ext_primary_key, array[1].replace("notnull", "not null"))
            else:
                text += "   {0}    BIGINT default nextval('{1}.seq_{2}')    {3}".format(array[0], schemaname, ext_primary_key,
                                                                                    array[1].replace("notnull",
                                                                                                     "not null"))
        elif line.find("constraint") > -1:
            primary_key = line[line.find("(") + 1:line.find(")")]
            ext_primary_key = line[line.find("(") + 1:line.find(")")]
            if primary_key == "data_id":
                ext_primary_key = tablename.replace("data", "") + primary_key
            elif primary_key == "extend_id":
                ext_primary_key = tablename.replace("extend", "") + primary_key
            text += "   constraint pk_{0} primary key ({1})\r".format(ext_primary_key, primary_key)
            if schemaname == "":
                drop_sequence += "drop sequence if exists seq_{0};\r".format(ext_primary_key)
                create_sequence += "create sequence seq_{0}  start 1001;\r".format(ext_primary_key)
            else:
                drop_sequence += "drop sequence if exists {0}.seq_{1};\r".format(schemaname, ext_primary_key)
                create_sequence += "create sequence {0}.seq_{1}  start 1001;\r".format(schemaname, ext_primary_key)
        else:
            text += line
    else:
        break
text = drop_view + "\r" + drop_table + "\r" + drop_sequence+ "\r" + create_sequence + "\r" + text.replace("\n\n\n","")
scriptFile.close()

newScripFile = open("D:\\DataBase\\Script\\tobacco_new.sql", mode="w", encoding="UTF-8")
newScripFile.write(text)
newScripFile.close()