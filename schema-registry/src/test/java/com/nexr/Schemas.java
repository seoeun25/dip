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
    public static final String employee_schema = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";

}
