package resp;

import static resp.RespConstants.CRLF;

public record RespSimpleString(String inputString) implements RespData{
    @Override
    public String toRespString() {
        return "+" + inputString + CRLF;
    }

    @Override
    public String toString() {
        return inputString;
    }
}
