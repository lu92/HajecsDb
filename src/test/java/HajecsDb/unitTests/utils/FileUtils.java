package HajecsDb.unitTests.utils;

import java.io.FileOutputStream;
import java.io.IOException;

public class FileUtils {
    public static void clearFile(String filename) {
        try {
            FileOutputStream writer = new FileOutputStream(filename);
            writer.write(("").getBytes());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
