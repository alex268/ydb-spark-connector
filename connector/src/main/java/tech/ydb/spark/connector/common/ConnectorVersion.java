package tech.ydb.spark.connector.common;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.utils.Version;

public class ConnectorVersion {
    private static final Logger logger = LoggerFactory.getLogger(ConnectorVersion.class);

    private final String version;
    private final String sdk;

    private ConnectorVersion(String version, String sdk) {
        this.version = version;
        this.sdk = sdk;
    }

    public String getConnectorVersion() {
        return version;
    }

    public String getFullVersion() {
        return "v" + version + " (based on SDK v" + sdk + ")";
    }

    public static ConnectorVersion getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final String PROPERTIES_PATH = "/ydb_spark.properties";
        private static final ConnectorVersion INSTANCE;

        static {
            String version = "0.0-dev";
            String sdk = "unknown";
            try (InputStream in = ConnectorVersion.class.getResourceAsStream(PROPERTIES_PATH)) {
                if (in != null) {
                    Properties prop = new Properties();
                    prop.load(in);
                    version = prop.getProperty("version", version);
                }
                sdk = Version.getVersion().orElse("unknown");
            } catch (Exception ex) {
                logger.warn("cannot load ydb connector version", ex);
            }

            INSTANCE = new ConnectorVersion(version, sdk);
        }
    }
}
