package org.apache.flume.interceptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.interceptor.CSVHeaderInterceptor.Constants.*;

/**
 * Interceptor class that appends the header from a CSV file to all
 * events.  It is always assumed the header is the first line from the
 * file.
 *
 * It is up to the user if they wish to point to the actual data file
 * with the header, or place the header in a separate file.  It will
 * not matter to this plugin.
 *
 * Properties:
 *
 *   key: Key to use in static header insertion (default is "csvheader")
 *
 *   preserveExisting: Whether to preserve an existing value for 'key'
 *                     (default is true)
 *
 *   file: File to retrieve header from (required)
 *
 * Sample config:
 *
 *   a1.sources.r1.interceptors = i1
 *   a1.sources.r1.interceptors.i1.type = org.apache.flume.interceptor.CSVHeaderInterceptor$Builder
 *   a1.sources.r1.interceptors.i1.key = csvheader
 *   a1.sources.r1.interceptors.i1.preserveExisting = true
 *   a1.sources.r1.interceptors.i1.file = /tmp/foobar
 *
 */
public class CSVHeaderInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
      .getLogger(CSVHeaderInterceptor.class);

  private final boolean preserveExisting;
  private final String key;
  private final String value;

  /**
   * Only {@link HostInterceptor.Builder} can build me
   */
  private CSVHeaderInterceptor(boolean preserveExisting, String key,
      String value) {
    this.preserveExisting = preserveExisting;
    this.key = key;
    this.value = value;
  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();

    if (preserveExisting && headers.containsKey(key)) {
      return event;
    }

    if (value != null) {
      headers.put(key, value);
    }
    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instance of the CSVHeaderInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private boolean preserveExisting;
    private String key;
    private String file;
    private String value;

    @Override
    public void configure(Context context) throws FlumeException {
      preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DEFAULT);
      key = context.getString(KEY, KEY_DEFAULT);
      file = context.getString(FILE);
      if (file != null) {
	  try {
	    BufferedReader headerfile = new BufferedReader(new FileReader(file));
	    value = headerfile.readLine();
	    logger.debug(String.format("CSV Header for file %s = %s", file, value));
	  }
	  catch (FileNotFoundException e) {
	    logger.error(String.format("CSVHeaderInterceptor - file not found = %s", file));
	    throw new FlumeException(String.format("CSVHeaderInterceptor - file not found = %s", file));
	  }
	  catch (Exception e) {
	    logger.error(String.format("CSVHeaderInterceptor - IO exception, file = %s", file));
	    throw new FlumeException(String.format("CSVHeaderInterceptor - IO exception, file = %s", file));
	  }
      }
      else {
	logger.error("CSVHeaderInterceptor - file not specified");
	throw new FlumeException("CSVHeaderInterceptor - file not specified");
      }
    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating CSVHeaderInterceptor: preserveExisting=%s,key=%s,value=%s",
          preserveExisting, key, value));
      return new CSVHeaderInterceptor(preserveExisting, key, value);
    }


  }

  public static class Constants {

    public static final String KEY = "key";
    public static final String KEY_DEFAULT = "key";

    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DEFAULT = true;

    public static final String FILE = "file";
  }
}
