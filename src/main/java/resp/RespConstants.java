package resp;

public class RespConstants {
    public static final String CRLF = "\r\n";
    public static final RespSimpleString PONG = new RespSimpleString("PONG");
    public static final RespBulkString NULLBULKRESP = new RespBulkString("_");
    public static final RespSimpleString OK = new RespSimpleString("OK");
}
