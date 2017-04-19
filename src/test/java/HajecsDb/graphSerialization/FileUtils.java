package HajecsDb.graphSerialization;

import java.io.FileOutputStream;
import java.io.IOException;

class FileUtils {
    static void clearFile(String filename) {
        try {
            FileOutputStream writer = new FileOutputStream(filename);
            writer.write(("").getBytes());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
