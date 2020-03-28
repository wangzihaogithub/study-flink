/*
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

package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Skeleton code for the datastream walkthrough
 *
 * 流处理
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/getting-started/walkthroughs/datastream_api.html
 *
 * 批处理
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/batch/examples.html
 *
 */
public class WordCountJob {
	public static void main(String[] args) throws Exception {
		String outputPath = System.getProperty("user.dir");
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.readTextFile(System.getProperty("user.dir")+"/files/wordcount-input.txt");

		DataSet<Tuple2<String, Integer>> counts =
				// 把每一行文本切割成二元组，每个二元组为: (word,1)
				text.flatMap(new Tokenizer())
						// 根据二元组的第“0”位分组，然后对第“1”位求和
						.groupBy(0)
						.sum(1);

		counts.writeAsCsv(outputPath, "\n", " ");
	}

	// 自定义函数
	public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// 统一大小写并把每一行切割为单词
			String[] tokens = value.toLowerCase().split("\\W+");

			// 消费二元组
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
