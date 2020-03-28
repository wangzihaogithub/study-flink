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

package stream.spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 * 欺诈检测器 v2：状态 + 时间 = ❤️
 *
 * 骗子们在小额交易后不会等很久就进行大额消费，这样可以降低小额测试交易被发现的几率。
 * 比如，假设你为欺诈检测器设置了一分钟的超时，对于上边的例子，交易 3 和 交易 4 只有间隔在一分钟之内才被认为是欺诈交易。
 * Flink 中的 KeyedProcessFunction 允许您设置计时器，该计时器在将来的某个时间点执行回调函数。
 *
 */
public class FraudDetector2 extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private transient ValueState<Boolean> flagState;
	private transient ValueState<Long> timerState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();


		//对于每笔交易，欺诈检测器都会检查该帐户的标记状态。 请记住，ValueState 的作用域始终限于当前的 key，即信用卡帐户。 如果标记状态不为空，则该帐户的上一笔交易是小额的，因此，如果当前这笔交易的金额很大，那么检测程序将输出报警信息。
		//
		//在检查之后，不论是什么状态，都需要被清空。 不管是当前交易触发了欺诈报警而造成模式的结束，还是当前交易没有触发报警而造成模式的中断，都需要重新开始新的模式检测。
		//
		//最后，检查当前交易的金额是否属于小额交易。 如果是，那么需要设置标记状态，以便可以在下一个事件中对其进行检查。
		// 注意，ValueState<Boolean> 实际上有 3 种状态：unset (null)，true，和 false，ValueState 是允许空值的。
		// 我们的程序只使用了 unset (null) 和 true 两种来判断标记状态被设置了与否。

		// Check if the flag is set
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				//Output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}
			// Clean up our state
			cleanUp(context);
		}


		//骗子们在小额交易后不会等很久就进行大额消费，这样可以降低小额测试交易被发现的几率。 比如，假设你为欺诈检测器设置了一分钟的超时，对于上边的例子，交易 3 和 交易 4 只有间隔在一分钟之内才被认为是欺诈交易。 Flink 中的 KeyedProcessFunction 允许您设置计时器，该计时器在将来的某个时间点执行回调函数。
		//
		//让我们看看如何修改程序以符合我们的新要求：
		//
		//当标记状态被设置为 true 时，设置一个在当前时间一分钟后触发的定时器。
		//当定时器被触发时，重置标记状态。
		//当标记状态被重置时，删除定时器。
		//要删除一个定时器，你需要记录这个定时器的触发时间，这同样需要状态来实现，所以你需要在标记状态后也创建一个记录定时器时间的状态。
		if (transaction.getAmount() < SMALL_AMOUNT) {
			// set the flag to true
			flagState.update(true);

			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);

			timerState.update(timer);
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// remove flag after 1 minute
		timerState.clear();
		flagState.clear();
	}

	private void cleanUp(Context ctx) throws Exception {
		// delete timer
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}
}
