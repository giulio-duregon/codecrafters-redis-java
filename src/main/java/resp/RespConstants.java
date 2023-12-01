package resp;

public class RespConstants {
    public static final String CRLF = "\r\n";
    public static final RespSimpleString PONG = new RespSimpleString("PONG");
    public static final RespBulkString NULLBULKRESP = new RespBulkString("_");
    public static final RespSimpleString OK = new RespSimpleString("OK");
    public static final String CONFIG = "config";
    public static final String PX = "px";
    public static final String EX = "ex";
    public static final String PING = "ping";
    public static final String ECHO = "echo";
    public static final String SET = "set";
    public static final String GET = "get";
    public static final String KEYS = "keys";

    public static final byte EOF = (byte) 0xFF;
    public static final byte SELECTDB = (byte) 0xFE;
    public static final byte EXPIRETIME = (byte) 0xFD;
    public static final byte EXPIRETIMEMS = (byte) 0xFC;
    public static final byte RESIZEDB = (byte) 0xFB;
    public static final byte AUX = (byte) 0xFA;

}
