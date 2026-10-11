package com.practice.consumer;

import java.util.Locale;

/**
 * 윈도우 콘솔이 UTF-8 로그를 CP949로 읽지 않도록, 기동 직후 코드 페이지를 65001로 바꾼다.
 */
final class WindowsConsoleUtf8 {

    private WindowsConsoleUtf8() {
    }

    static void enable() {
        // 리눅스 콘솔은 UTF-8이 기본이라 코드 페이지를 바꿀 필요가 없다.
        String os = System.getProperty("os.name", "");
        if (!os.toLowerCase(Locale.ROOT).contains("windows")) {
            return;
        }
        try {
            // chcp 65001은 이 콘솔이 이후 출력을 UTF-8로 읽게 한다.
            // inheritIO로 같은 콘솔에 적용해야 로그 창의 코드 페이지가 바뀐다.
            Process process = new ProcessBuilder("cmd.exe", "/c", "chcp", "65001")
                    .inheritIO()
                    .start();
            // 첫 로그보다 먼저 끝나야 해서, 코드 페이지 변경이 끝날 때까지 기동을 기다린다.
            process.waitFor();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // 콘솔이 없는 실행이면 코드 페이지를 바꿀 수 없다. 기동은 계속한다.
        }
    }
}
