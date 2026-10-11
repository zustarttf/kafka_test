package com.practice.consumer;

import java.util.Locale;

/** 윈도우 콘솔이 UTF-8 로그를 CP949로 읽지 않도록 코드 페이지를 65001로 바꾼다. */
final class WindowsConsoleUtf8 {

    private WindowsConsoleUtf8() {
    }

    static void enable() {
        String os = System.getProperty("os.name", "");
        if (!os.toLowerCase(Locale.ROOT).contains("windows")) {
            return;
        }
        try {
            Process process = new ProcessBuilder("cmd.exe", "/c", "chcp", "65001")
                    .inheritIO()
                    .start();
            process.waitFor();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
    }
}
