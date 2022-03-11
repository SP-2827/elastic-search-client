package in.toadsage.datastore.es;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ESAccessException extends RuntimeException {

    public ESAccessException(final String message, final Exception e) {
        super(message, e);
        log.error(message, e);
    }

    public ESAccessException(final Exception e, final String message, final Object... objects) {
        super(e);
        log.error(message, objects, e);
    }

}
