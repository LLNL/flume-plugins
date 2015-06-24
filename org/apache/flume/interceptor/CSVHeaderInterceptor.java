/*****************************************************************************\
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory
 * Written by Albert Chu <chu11@llnl.gov>
 * LLNL-CODE-673778
 * All rights reserved.
 * This file is part of flume-plugins. 
 *
 * For details, see https://github.com/chu11/flume-plugins. Please also
 * read project DISCLAIMER
 * (https://github.com/chu11/flume-plugins/blob/master/DISCLAIMER).
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the disclaimer below.
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the disclaimer (as noted below)
 *   in the documentation and/or other materials provided with the
 *   distribution.
 * - Neither the name of the LLNS/LLNL nor the names of its contributors
 *   may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LAWRENCE
 * LIVERMORE NATIONAL SECURITY, LLC, THE U.S. DEPARTMENT OF ENERGY OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
\*****************************************************************************/

package org.apache.flume.interceptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
 *   period: In seconds, period in which to check if headers in file
 *           have changed.  Predominantly useful if headers are in a
 *           separate file, instead of the file that also includes
 *           data.  Specify 0 to check on every event and -1 to never
 *           check. (default is -1)
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
  private final String file;
  private String value;
  private int period;
  private long nextheaderscan;

  /**
   * Only {@link HostInterceptor.Builder} can build me
   */
  private CSVHeaderInterceptor(boolean preserveExisting, String key,
			       String value, String file, int period) {
    this.preserveExisting = preserveExisting;
    this.key = key;
    this.value = value;
    this.file = file;
    this.period = period;
    if (period > 0) {
	calcNextheaderscan();
    }
  }

  private void calcNextheaderscan() {
      this.nextheaderscan = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(period);
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

    if (period >= 0) {
	if (period == 0 || System.currentTimeMillis() > this.nextheaderscan) {
	    try {
		String tmpValue = readHeader(file);
		if (tmpValue != value) {
		    logger.debug(String.format("New CSV Header for file %s = %s", file, value));
		    value = tmpValue;
		}
	    }
	    catch (FlumeException e) {
		logger.error(String.format("Error reading new CSV Header, keeping old one = %s", value));
	    }
	    calcNextheaderscan();
	}
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

  private static String readHeader(String file) throws FlumeException {
      try {
	  BufferedReader headerfile = new BufferedReader(new FileReader(file));
	  String value = headerfile.readLine();
	  logger.debug(String.format("CSV Header for file %s = %s", file, value));
	  return value;
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

  /**
   * Builder which builds new instance of the CSVHeaderInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private boolean preserveExisting;
    private String key;
    private String file;
    private String value;
    private Integer period;

    @Override
    public void configure(Context context) throws FlumeException {
      preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DEFAULT);
      key = context.getString(KEY, KEY_DEFAULT);
      file = context.getString(FILE);
      period = context.getInteger(PERIOD, new Integer(PERIOD_DEFAULT));
      if (file != null) {
	  value = readHeader(file);
      }
      else {
	logger.error("CSVHeaderInterceptor - file not specified");
	throw new FlumeException("CSVHeaderInterceptor - file not specified");
      }
    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating CSVHeaderInterceptor: preserveExisting=%s,key=%s,value=%s,period=%s",
          preserveExisting, key, value, file, period.intValue()));
      return new CSVHeaderInterceptor(preserveExisting, key, value, file, period);
    }


  }

  public static class Constants {

    public static final String KEY = "key";
    public static final String KEY_DEFAULT = "key";

    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DEFAULT = true;

    public static final String PERIOD = "period";
    public static final int PERIOD_DEFAULT = -1;

    public static final String FILE = "file";
  }
}
