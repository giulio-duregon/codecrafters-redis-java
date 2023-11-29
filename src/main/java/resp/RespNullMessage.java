package resp;

public record RespNullMessage() implements RespData {
    @Override
    public String toRespString() {
        return "$-1\r\n";
    }

    @Override
    public String toString() {
        return null;
    }
}
