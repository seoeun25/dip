package com.nexr;

public class Schemas {

    public static final String gpx_port = "gpx_port";
    public static final String gpx_port_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"gpx_port\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"nescode\", \"type\": \"string\"},\n" +
            "     {\"name\": \"equip_ip\", \"type\": \"string\"},\n" +
            "     {\"name\": \"port\", \"type\": \"string\"},\n" +
            "     {\"name\": \"setup_val\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"nego\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"setup_speed\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"curnt_speed\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"mac_cnt\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"downl_speed_val\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"etc_extrt_info\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"wrk_dt\", \"type\": \"long\"},\n" +
            "     {\"name\": \"src_info\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";
    public static final String ftth_if = "ftth_if";
    public static final String ftth_if_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"ftth_if\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"ontmac\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";
    public static final String employee = "employee";
    public static final String employee_schema_test = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"employee\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"wrk_dt\", \"type\": \"long\"},\n" +
            "     {\"name\": \"src_info\", \"type\": \"string\"},\n" +
            "     {\n" +
            "         \"name\": \"header\",\n" +
            "         \"type\": {\n" +
            "             \"type\" : \"record\",\n" +
            "             \"name\" : \"headertype\",\n" +
            "             \"fields\" : [\n" +
            "                 {\"name\": \"time\", \"type\": \"long\"}\n" +
            "             ]\n" +
            "         }\n" +
            "      }\n" +
            " ]\n" +
            "}";

    public static final String employee_schema1 = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";

    //same schema - remove line delimiter from employee_schema1
    public static final String employee_schema2 = "{\"namespace\": \"example.avro\", \"type\": \"record\",\"name\": \"User\", \"fields\": " +
            "[{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} ]}";

    //different schema - namespace
    public static final String employee_schema3 = "{\"namespace\": \"example.hello\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";

    // different schema - nullable
    public static final String employee_schema4 = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";
}
