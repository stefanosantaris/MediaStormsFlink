package cdvc;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Collection;
import java.util.List;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class MediaStormIndexing {

	private static String outputTableName = HBaseFlinkTestConstants.TEST_TABLE_NAME;

	//
	// Program
	//

	public static void main(String[] args) throws Exception {

		String hostName = "";
		Integer port = 0;

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		DataStream<Tuple2<String, String>> dataStream = env
				.socketTextStream(hostName, port)
				.flatMap(new DimensionSorter()).groupBy(0)
				.flatMap(new ImageComparison()).sortGroup(1, Order.ASCENDING);

		storeToDatabase(dataStream);

	}

	private static void storeToDatabase(
			DataStream<Tuple2<String, String>> dataStream) {
		// emit result
		Job job = Job.getInstance();
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
				outputTableName);
		// TODO is "mapred.output.dir" really useful?
		job.getConfiguration().set("mapred.output.dir",
				HBaseFlinkTestConstants.TMP_DIR);
		dataStream
				.map(new RichMapFunction<Tuple2<String, String>, Tuple2<Text, Mutation>>() {
					private transient Tuple2<Text, Mutation> reuse;

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);
						reuse = new Tuple2<Text, Mutation>();
					}

					@Override
					public Tuple2<Text, Mutation> map(Tuple2<String, String> t)
							throws Exception {
						reuse.f0 = new Text(t.f0);
						Put put = new Put(t.f0.getBytes());
						put.add(HBaseFlinkTestConstants.CF_SOME,
								HBaseFlinkTestConstants.Q_SOME,
								Bytes.toBytes(t.f1));
						reuse.f1 = put;
						return reuse;
					}
				}).output(
						new HadoopOutputFormat<Text, Mutation>(
								new TableOutputFormat<Text>(), job));
	}

	public static class ImageComparison implements
			FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		@Override
		public void flatMap(Tuple2<String, String> reorderdImageVectors,
				Collector<Tuple2<String, String>> out) throws Exception {
			String pk = reorderdImageVectors.f0;
			DataSet<Tuple1<String>> W = retrieveStoredImagesWithTheSamePK(pk);
			W.collect().add(new Tuple1<String>(reorderdImageVectors.f1));
			for (int i = 0; i < W.collect().size(); i++) {
				out.collect(new Tuple2<String, String>(reorderdImageVectors.f1,
						W.collect().get(i).f0));
			}
		}

		private DataSet<Tuple1<String>> retrieveStoredImagesWithTheSamePK(
				String pk) {
			ExecutionEnvironment env1 = ExecutionEnvironment
					

			DataSet<Tuple1<String>> hbaseDs = env1
					.createInput(new TableInputFormat<Tuple2<String, String>>() {

						@Override
						public String getTableName() {
							return HBaseFlinkTestConstants.TEST_TABLE_NAME;
						}

						@Override
						protected Scan getScanner() {
							Scan scan = new Scan();
							scan.addColumn(HBaseFlinkTestConstants.CF_SOME,
									HBaseFlinkTestConstants.Q_SOME);
							return scan;
						}

						private Tuple1<String> storedImages = new Tuple1<String>();

						@Override
						protected Tuple1<String> mapResultToTuple(Result r) {
							String key = pk;
							String val = Bytes.toString(r.getValue(
									HBaseFlinkTestConstants.CF_SOME,
									HBaseFlinkTestConstants.Q_SOME));
							storedImages.setField(val, 0);
							return storedImages;
						}
					});
		}
	}

	public static class DimensionSorter implements
			FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String imageVector,
				Collector<Tuple2<String, String>> out) throws Exception {
			ExecutionEnvironment env1 = ExecutionEnvironment
					.getExecutionEnvironment();
			DataSet<Tuple1<String>> priorityIndex = retrievePriorityIndex(env1);
			String[] imageVectorSplit = imageVector.split(" ");
			String[] priorityIndexSplit = priorityIndex.collect().get(0).f0
					.split(" ");
			String[] reorderedVectorSplit = new String[imageVectorSplit.length];
			int counter = 0;
			for (String priority : priorityIndexSplit) {
				int index = Integer.parseInt(priority);
				reorderedVectorSplit[counter] = imageVectorSplit[index];
				counter++;
			}
			String finalVector = "";
			for (String value : reorderedVectorSplit) {
				finalVector += value + " ";
			}

			out.collect(new Tuple2<String, String>(reorderedVectorSplit[0],
					finalVector));
		}

		private static DataSet<Tuple1<String>> retrievePriorityIndex(
				ExecutionEnvironment env1) {
			DataSet<Tuple1<String>> hbaseDs = env1
					.createInput(new TableInputFormat<Tuple2<String, String>>() {

						@Override
						public String getTableName() {
							return HBaseFlinkTestConstants.TEST_TABLE_NAME;
						}

						@Override
						protected Scan getScanner() {
							Scan scan = new Scan();
							scan.addColumn(HBaseFlinkTestConstants.CF_SOME,
									HBaseFlinkTestConstants.Q_SOME);
							return scan;
						}

						private Tuple1<String> priorityIndex = new Tuple1<String>();

						@Override
						protected Tuple1<String> mapResultToTuple(Result r) {
							String key = "prorityIndex";
							String val = Bytes.toString(r.getValue(
									HBaseFlinkTestConstants.CF_SOME,
									HBaseFlinkTestConstants.Q_SOME));
							priorityIndex.setField(val, 0);
							return priorityIndex;
						}
					});
		}
	}
}
