package resp;

import static resp.RespConstants.CRLF;

public record RespBulkString(String inputString) implements RespData {
    @Override
    public String toRespString() {
        return "$"+ inputString.length()+CRLF + inputString + CRLF;
    }

    @Override
    public String toString() {
        return inputString;
    }
}
