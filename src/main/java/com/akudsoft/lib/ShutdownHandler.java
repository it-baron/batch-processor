package com.akudsoft.lib;


import java.util.Stack;

public final class ShutdownHandler {
    public static void init() {

    }

    public interface Shutdown {
        void execute() throws Exception;
    }

    private final static Stack<Shutdown> list = new Stack<Shutdown>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ShutdownHandler.shutdown();
            }
        });
    }

    public static void addAction(Shutdown action) {
        list.push(action);
    }

    private static void shutdown() {
        while (!list.empty()) {
            try {
                list.pop().execute();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
