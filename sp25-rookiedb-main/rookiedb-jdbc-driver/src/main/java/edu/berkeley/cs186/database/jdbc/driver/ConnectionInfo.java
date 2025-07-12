package edu.berkeley.cs186.database.jdbc.driver;

public class ConnectionInfo {


    private String host;
    private int port = 8080;
    private String databasePath;
    private int bufferSize = 262144; // 默认 1GB
    private boolean enableLocking = false;
    private boolean enableRecovery = false;
    private int workMem = 1024; // 默认 4MB

    // Getters and Setters
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabasePath() {
        return databasePath;
    }

    public void setDatabasePath(String databasePath) {
        this.databasePath = databasePath;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isEnableLocking() {
        return enableLocking;
    }

    public void setEnableLocking(boolean enableLocking) {
        this.enableLocking = enableLocking;
    }

    public boolean isEnableRecovery() {
        return enableRecovery;
    }

    public void setEnableRecovery(boolean enableRecovery) {
        this.enableRecovery = enableRecovery;
    }

    public int getWorkMem() {
        return workMem;
    }

    public void setWorkMem(int workMem) {
        this.workMem = workMem;
    }

    @Override
    public String toString() {
        return "ConnectionInfo{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", databasePath='" + databasePath + '\'' +
                ", bufferSize=" + bufferSize +
                ", enableLocking=" + enableLocking +
                ", enableRecovery=" + enableRecovery +
                ", workMem=" + workMem +
                '}';
    }

}
