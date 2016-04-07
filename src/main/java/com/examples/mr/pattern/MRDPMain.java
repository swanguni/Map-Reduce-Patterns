package com.examples.mr.pattern;

import java.util.Arrays;

import com.examples.mr.pattern.comment_count.*;

import com.examples.mr.pattern.*;

import com.examples.mr.pattern.summarization.*;
import com.examples.mr.pattern.filtering.*;
import com.examples.mr.pattern.joins.*;
import com.examples.mr.pattern.meta.*;
import com.examples.mr.pattern.inout.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("unused")
public class MRDPMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MRDPMain(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length > 0) {
			String example = args[0];
			String[] otherArgs = Arrays.copyOfRange(args, 1, args.length);

			if (example.equalsIgnoreCase("PartitionPruningOutput")) {
				PartitionPruningOutputDriver.main(otherArgs);
			} else if (example.equalsIgnoreCase("PartitionPruningInput")) {
				PartitionPruningInputDriver.main(otherArgs);
			} else if (example.equalsIgnoreCase("RedisInput")) {
				RedisInputDriver.main(otherArgs);
			} else if (example.equalsIgnoreCase("RedisOutput")) {
				RedisOutputDriver.main(otherArgs);
			} else {
				printHelp();
				return 1;
			}

			return 0;
		} else {
			printHelp();
			return 1;
		}
	}

	private void printHelp() {
		System.out
				.println("Usage: hadoop jar mrdp.jar <example> <example args>");
		System.out.println("Examples are:");
		System.out.println("Chapter 7:");
		System.out
				.println("\tRedisOutput  <user data> <redis hosts> <hashset name>");
		System.out
				.println("\tRedisInput <redis hosts> <hashset name> <output>");
		System.out.println("\tPartitionPruningOutput <user data>");
		System.out
				.println("\tPartitionPruningInput <last access months> <output>");
	}
}
