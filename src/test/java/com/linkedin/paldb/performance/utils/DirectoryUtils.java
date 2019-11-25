package com.linkedin.paldb.performance.utils;

import java.io.File;
import java.util.Objects;


public class DirectoryUtils {

  public static boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (final File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
    }
    return (directory.delete());
  }

  public static long folderSize(File directory) {
    long length = 0;
    for (File file : Objects.requireNonNull(directory.listFiles())) {
      if (file == null) break;
      if (file.isFile()) {
        length += file.length();
      } else {
        length += folderSize(file);
      }
    }
    return length;
  }
}
