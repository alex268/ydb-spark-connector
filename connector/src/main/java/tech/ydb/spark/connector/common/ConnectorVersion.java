package tech.ydb.spark.connector.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import tech.ydb.core.utils.Version;

public class ConnectorVersion {
    private final String version;
    private final String sdk;

    private ConnectorVersion(String version) {
        this.version = version;
        this.sdk = Version.getVersion().orElse("unknown");
    }

    public String getConnectorVersion() {
        return version;
    }

    public String getFullVersion() {
        return "v" + version + "(based on SDK v" + sdk + ")";
    }

    public static ConnectorVersion getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final String PROPERTIES_PATH = "/ydb_spark.properties";
        private static final ConnectorVersion INSTANCE;

        static {
            String version = "0.0-dev";
            try (InputStream in = Version.class.getResourceAsStream(PROPERTIES_PATH)) {
                Properties prop = new Properties();
                prop.load(in);
                version = prop.getProperty("version");
            } catch (IOException e) { }

            INSTANCE = new ConnectorVersion(version);
        }
    }
}
