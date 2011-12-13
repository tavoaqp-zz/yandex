package org.yandex.estimate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class DocEvent implements Writable{
	
	private IntWritable id;
	private IntWritable query_id;
	private IntWritable session_id;
	private IntWritable time;
	private IntWritable click_distance;
	private IntWritable position;
	private IntWritable region;
	private BooleanWritable clicked;

	public DocEvent(){
		id=new IntWritable();
		query_id=new IntWritable();
		session_id=new IntWritable();
		time=new IntWritable();
		click_distance=new IntWritable();
		position=new IntWritable();
		region=new IntWritable();
		clicked=new BooleanWritable(); 
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		id.readFields(arg0);
		query_id.readFields(arg0);
		session_id.readFields(arg0);
		time.readFields(arg0);
		click_distance.readFields(arg0);
		position.readFields(arg0);
		region.readFields(arg0);
		clicked.readFields(arg0);
	}

	public String toString(){
		return "id: "+id+", query_id: "+query_id.get()+", session_id: "+session_id
				+", time:"+time.get()+
				", distance:"+click_distance.get()+", pos: "+position.get()+", region: "+region.get()+
				", click: "+clicked.get();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		id.write(arg0);
		query_id.write(arg0);
		session_id.write(arg0);
		time.write(arg0);
		click_distance.write(arg0);
		position.write(arg0);
		region.write(arg0);
		clicked.write(arg0);
	}

	public int getId() {
		return id.get();
	}

	public void setId(int id) {
		this.id.set(id);
	}

	public int getQuery_id() {
		return query_id.get();
	}

	public void setQuery_id(int query_id) {
		this.query_id.set(query_id);
	}

	public int getSession_id() {
		return session_id.get();
	}

	public void setSession_id(int session_id) {
		this.session_id.set(session_id);
	}

	public int getTime() {
		return time.get();
	}

	public void setTime(int time) {
		this.time.set(time);
	}

	public int getClick_distance() {
		return click_distance.get();
	}

	public void setClick_distance(int click_distance) {
		this.click_distance.set(click_distance);
	}

	public int getPosition() {
		return position.get();
	}

	public void setPosition(int position) {
		this.position.set(position);
	}

	public int getRegion() {
		return region.get();
	}

	public void setRegion(int region) {
		this.region.set(region);
	}

	public boolean getClicked() {
		return clicked.get();
	}

	public void setClicked(boolean clicked) {
		this.clicked.set(clicked);
	}

	public boolean getJustResult() {
		return !getClicked();
	}
}
