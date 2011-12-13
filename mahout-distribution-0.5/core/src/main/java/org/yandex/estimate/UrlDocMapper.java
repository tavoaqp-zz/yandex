package org.yandex.estimate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlDocMapper extends Mapper<WritableComparable, Text, Text, DocObservations> {

	private final Logger log = LoggerFactory.getLogger(UrlDocMapper.class);
	
	public static String QUERY_TYPE = "Q";
	public static String CLICK_TYPE = "C";

	public static int SESSION_ID = 0;
	public static int TIME = 1;
	public static int ACTION_TYPE = 2;
	public static int QUERY_ID = 3;
	public static int REGION_ID = 4;
	public static int DOC_START_POS = 5;
	public static int URL_ID=3;
	
	@Override
	public void map(WritableComparable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		String line=value.toString();
		String[] splits=line.split("\\t");
		String actionType = splits[ACTION_TYPE];
		String session_id_str=splits[SESSION_ID];
		int session_id=Integer.parseInt(session_id_str);		
		
		if (actionType.equals(QUERY_TYPE)) {
			int query_id=Integer.parseInt(splits[QUERY_ID]);
			int time=Integer.parseInt(splits[TIME]);
			int region=Integer.parseInt(splits[REGION_ID]);
			for (int i=DOC_START_POS;i<splits.length;i++)
			{
				int url_id=Integer.parseInt(splits[i]);
				DocEvent urlDoc=new DocEvent();
				urlDoc.setId(url_id);
				urlDoc.setClicked(false);
				urlDoc.setClick_distance(0);
				urlDoc.setPosition(i-DOC_START_POS+1);
				urlDoc.setQuery_id(query_id);
				urlDoc.setRegion(region);
				urlDoc.setSession_id(session_id);
				urlDoc.setTime(time);
				DocObservations list=new DocObservations();
				list.set(new DocEvent[]{urlDoc});
				context.write(new Text(session_id_str), list);
			}				
		} 
		else
		{
			DocEvent urlDoc=new DocEvent();
			urlDoc.setId(Integer.parseInt(splits[URL_ID]));
			urlDoc.setTime(Integer.parseInt(splits[TIME]));
			urlDoc.setQuery_id(0);
			urlDoc.setClicked(true);
			DocObservations list=new DocObservations();
			list.set(new DocEvent[]{urlDoc});
			context.write(new Text(session_id_str), list);
		}
	}

		
	
}