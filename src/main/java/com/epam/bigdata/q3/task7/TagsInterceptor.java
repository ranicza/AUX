package com.epam.bigdata.q3.task7;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

public class TagsInterceptor implements Interceptor{
	
	private static final Logger log = LoggerFactory.getLogger(TagsInterceptor.class);

    private static final String PATH_FILE = "/tmp/admin/hw7/user.profile.tags.us.tag.txt";
    private static final String PATH = "hdfs://sandbox.hortonworks.com:8020";
    private static final String TAB = "\\t";
    private static final String HEADER_EVENT_DATE = "tms_date";
    private static final String HEADER_TAGS = "tags";
    private static final String TRUE = "true";
    private static final String FALSE ="false";
    private static final String EMPTY =  "";
    private static final String DELIMETER = "\t";
    
	private Map<String, String> tags;

	@Override
    public void initialize() {
        getTags();
        log.info("Interceptor initialized.");
    }

	@Override
	public Event intercept(Event event) {
		List<String> fields = null;
		Map<String, String> headers = event.getHeaders();
		String body = new String(event.getBody());

		if (StringUtils.isNotEmpty(body) && StringUtils.isNotBlank(body)) {
			fields = new ArrayList<>(Arrays.asList(body.split(TAB)));

			String tagsId = fields.get(fields.size() - 2);
			String userTags = tags.getOrDefault(tagsId, EMPTY);
			headers.put(HEADER_TAGS, StringUtils.isNotBlank(userTags) ? TRUE : FALSE);
			fields.add(userTags);

			// Get event date for partition
			String dateEvent = fields.get(1).substring(0, 8);
			headers.put(HEADER_EVENT_DATE, dateEvent);

			event.setHeaders(headers);
			event.setBody(String.join(DELIMETER, fields).getBytes());
		}
		return event;
	}

    @Override
    public java.util.List<Event> intercept(java.util.List<Event> events) {
        List<Event> intEvents = new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event intEvent = intercept(event);
            intEvents.add(intEvent);
        }
        return intEvents;
    }

    @Override
    public void close() {
        log.info("Interceptor closed.");
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public void configure(Context context) {
        }

        @Override
        public Interceptor build() {
            return new TagsInterceptor();
        }
    }

    private void getTags() {
    	List<String> lines = new ArrayList<>();
        String line;
        
    	Configuration config = new Configuration();       
        Path path = new Path(PATH_FILE);
        FileSystem fs = null;
        BufferedReader br = null;
        
        try {
        	fs = FileSystem.get(new URI(PATH), config);
            br = new BufferedReader(new InputStreamReader(fs.open(path)));

            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
            this.tags = lines.stream()
                    .skip(1)
                    .map(s -> s.split(TAB))
                    .collect(Collectors.toMap(
                            row -> row[0],
                            row -> row[1]
                    ));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
