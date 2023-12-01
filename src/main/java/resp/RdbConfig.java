package resp;

import java.io.File;

public record RdbConfig(String dir, String fileName) {
    public File getFilePath() {
        return new File(dir, fileName);
    }
}
