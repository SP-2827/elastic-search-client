package in.toadsage.datastore.es;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


@Slf4j
public class PropertyHandler {


    private static final String CONFIG_PROPERTIES = "config.properties";
    private static final Map<String, String> PROPS;

    static {
        try (var input = PropertyHandler.class.getClassLoader().getResourceAsStream(CONFIG_PROPERTIES)) {

            if (input == null) {
                log.error("Sorry, unable to find {}", CONFIG_PROPERTIES);
            }
            var prop = new Properties();
            //load a properties file from class path, inside static method
            prop.load(input);
            var maps = prop.entrySet().stream().collect(
                    Collectors.toMap(
                            e -> String.valueOf(e.getKey()),
                            e -> String.valueOf(e.getValue()),
                            (prev, next) -> next, HashMap::new
                    ));
            PROPS = Collections.unmodifiableMap(maps);
        } catch (IOException e) {
            throw new ESAccessException(e, "Sorry, unable to load {}", CONFIG_PROPERTIES);
        }
    }

    private PropertyHandler() {
    }

    public static String get(final String key) {
        return PROPS.get(key);
    }

}
