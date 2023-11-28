package resp;

import java.util.List;
import java.util.stream.Collectors;

import static resp.RespConstants.CRLF;

public record RespArray(List<RespData> dataArray) implements RespData{
    @Override
    public String toString() {
        return "[" + dataArray.stream().map(RespData::toString).collect(Collectors.joining(",")) + "]";
    }

    public RespBulkString popFront(){
        return (RespBulkString) dataArray.remove(0);
    }

    @Override
    public String toRespString() {
        return "*" + dataArray.size() + CRLF +
                dataArray.stream()
                        .map(RespData::toRespString)
                        .collect(Collectors.joining("")
                        );
    }
}
