scriptFile = open("d:\\crebas.sql", "r")

text = ""
drop_sequence= ""
create_sequence= ""
tablename=""
drop_view=""
drop_table=""
while True:
    line = scriptFile.readline()
    if line:
        if line.find("_id") > -1:
            line = line.replace("INT4", "BIGINT").replace("integer", "BIGINT")
        if line.find("=========") > -1:
            pass
        elif line.find("DBMS name:") > -1:
            pass
        elif line.find("Created on:") > -1:
            pass
        if line.find("drop view") > -1:
            drop_view += line
        if line.find("drop table") > -1:
            drop_table += line
        elif line.find(" DATE ") > -1:
            text += line.replace(" DATE ","timestamp").replace("CURRENT_DATE","CURRENT_TIMESTAMP")
        elif line.find("create table ") > -1:
            tablename = line[line.find("create table ") + 13:line.find(" (")]
            text += line
        elif line.find("SERIAL") > -1:
            array = line.replace(" ","").split("SERIAL")
            primary_key = array[0]
            ext_primary_key = array[0]
            if primary_key == "data_id":
                ext_primary_key = tablename.replace("data", "") + primary_key
            elif primary_key == "extend_id":
                ext_primary_key = tablename.replace("extend", "") + primary_key
            text += "   {0}    BIGINT default nextval('seq_{1}')    {2}".format(array[0], ext_primary_key, array[1].replace("notnull", "not null"))
        elif line.find("constraint") > -1:
            primary_key = line[line.find("(") + 1:line.find(")")]
            ext_primary_key = line[line.find("(") + 1:line.find(")")]
            if primary_key == "data_id":
                ext_primary_key = tablename.replace("data", "") + primary_key
            elif primary_key == "extend_id":
                ext_primary_key = tablename.replace("extend", "") + primary_key
            text += "   constraint pk_{0} primary key ({1})\r".format(ext_primary_key, primary_key)
            drop_sequence += "drop sequence if exists seq_{0};\r".format(ext_primary_key)
            create_sequence += "create sequence seq_{0}  start 1001;\r".format(ext_primary_key)
        else:
            text += line
    else:
        break
text = drop_view + "\r" + drop_table + "\r" + drop_sequence+ "\r" + create_sequence + "\r" + text.replace("\n\n\n","")
scriptFile.close()

newScripFile = open("D://crebas2.sql", mode="w", encoding="UTF-8")
newScripFile.write(text)
newScripFile.close()