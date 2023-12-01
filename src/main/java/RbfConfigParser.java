import resp.RdbConfig;

import java.util.Optional;

public class RbfConfigParser {
    static Optional<RdbConfig> parseConfig(String[] args) {
        String dir = null;
        String fileName = null;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--dir") && (i + 1 < args.length)) {
                dir = args[i + 1];
            } else if (args[i].equalsIgnoreCase("--dbfilename") && (i + 1 < args.length)) {
                fileName = args[i + 1];
            }
        }

        if ((dir != null) && (fileName != null)) {
            return Optional.of(new RdbConfig(dir, fileName));
        }
        return Optional.empty();
    }
}
