package sbp.school.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import sbp.school.kafka.configuration.DataBaseConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomTransactionSourceConnector extends SourceConnector {

    private Map<String, String> props;

    @Override
    public String version() {

        return "version 1.0";
    }

    @Override
    public void start(Map<String, String> props) {

        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {

        return CustomTransactionSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }

        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {

        return DataBaseConfig.CONFIG_DEF;
    }
}
