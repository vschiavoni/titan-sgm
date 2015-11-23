/**
 * Author: Valerio Schiavoni <valerio.schiavoni@gmail.com>
 */
package ch.unine.iiun.titan.sgm;

import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

/**
 * General utils.
 */
public class Utils {

	public static String durationSince(long start, long stop) {
		Duration duration = new Duration(start, stop);

		PeriodFormatter formatter = new PeriodFormatterBuilder().appendDays()
				.appendSuffix("d").appendHours().appendSuffix("h")
				.appendMinutes().appendSuffix("m").appendSeconds()
				.appendSuffix("s").toFormatter();
		String formatted = formatter.print(duration.toPeriod());

		return formatted;
	}

}
